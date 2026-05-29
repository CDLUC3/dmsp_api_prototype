require 'base64'
require 'httparty'
require 'aws-sdk-dynamodb'
require 'aws-sdk-cloudformation'
require 'rainbow'

# Fetches all of the CloudFormation Stack Outputs (they must have been 'Exported')
def fetch_cf_stack_exports
  exports = []
  cf_client = Aws::CloudFormation::Client.new(region: @default_region)
  exports << cf_client.list_exports.exports
  exports.flatten
end

# Search the stack outputs for the name
def fetch_cf_output(name:)
  vals = @stack_exports.select do |exp|
    (name&.downcase&.strip == 'lambdasecuritygroupid' && exp.name.downcase.strip == 'lambdasecuritygroupid') ||
    ((exp.exporting_stack_id.include?(@prefix) || exp.exporting_stack_id.include?("#{@program}-#{@env}") ) &&
      "#{@env}-#{name&.downcase&.strip}" == exp.name.downcase.strip)
  end
  vals&.first&.value
end

# Fetch the authentication token
def auth
  scopes = [
    "https://auth.#{@domain}/#{@env}.read",
    "https://auth.#{@domain}/#{@env}.write",
    "https://auth.#{@domain}/#{@env}.delete",
    "https://auth.#{@domain}/#{@env}.upload"
  ]

  options = {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': "Basic #{Base64.strict_encode64("#{@client_id}:#{@client_secret}")}"
    },
    follow_redirects: true,
    limit: 3,
    body: "grant_type=client_credentials&scope=#{scopes.join(' ')}"
  }

  puts Rainbow("Authenticating ...").silver
  resp = HTTParty.post("https://auth.#{@domain}/oauth2/token", options)
  puts Rainbow("    Unable to authenticate! #{resp.code} - #{resp.body}").red unless resp.code == 200

  @token = resp.code == 200 ? resp['access_token'] : nil
end

# Verify that we can validate
def validate_dmp
  opts = @options.dup
  opts[:body] = @dmp.to_json
  puts Rainbow("Validating DMP ...").silver
  resp = HTTParty.post("https://api.#{@domain}/dmps/validate", opts)
  puts Rainbow("    Validation failure! #{resp.code} - #{resp['errors']}").red unless resp.code == 200
  resp.code == 200
end

# Verify that we can create
def create_dmp
  opts = @options.dup
  opts[:body] = @dmp.to_json
  puts Rainbow("Creating DMP ...").silver
  resp = HTTParty.post("https://api.#{@domain}/dmps", opts)
  puts Rainbow("    Creation failure! #{resp.code} - #{resp['errors']}").red unless resp.code == 201
  json = JSON.parse(resp.body)
  puts Rainbow("    Creation failure! Bad JSON returned").red if json.nil? ||
                                                                 json.fetch('items', []).compact.empty? ||
                                                                 json.fetch('items', []).first['dmp'].nil?
  resp.code == 201 ? json['items'].first : nil
end

# Verify that we can update
def update_dmp
  opts = @options.dup
  opts[:body] = @dmp.to_json
  puts Rainbow("Updating DMP ...").silver
  doi = @dmp_id.gsub('https://doi.org/', '')
  resp = HTTParty.put("https://api.#{@domain}/dmps/#{doi}", opts)
  puts Rainbow("    Update failure! #{resp.code} - #{resp['errors']}").red unless resp.code == 200
  json = JSON.parse(resp.body)
  puts Rainbow("    Update failure! Bad JSON returned").red if json.nil? ||
                                                               json.fetch('items', []).compact.empty? ||
                                                               json.fetch('items', []).first['dmp'].nil?
  resp.code == 200 ? json['items'].first : nil
end

# Verify that we can tombstone
def tombstone_dmp
  puts Rainbow("Tombstoning DMP ...").silver
  doi = @dmp_id.gsub('https://doi.org/', '')
  resp = HTTParty.delete("https://api.#{@domain}/dmps/#{doi}", @options)
  puts Rainbow("    Tombstone failure! #{resp.code} - #{resp['errors']}").red unless resp.code == 200
  json = JSON.parse(resp.body)
  puts Rainbow("    Tombstone failure! Bad JSON returned").red if json.nil? ||
                                                                  json.fetch('items', []).compact.empty? ||
                                                                  json.fetch('items', []).first['dmp'].nil?
  resp.code == 200 ? json['items'].first : nil
end

# Verify that the EzidPublisher Lambda was invoked
def verify_ezid_publication

end

# Verify that the Citer Lambda was invoked
def verify_citation
  pk = @dmp_id.gsub('https://', 'DMP#')
  resp = @dynamo.get_item({ table_name: @dmp_table, key: { PK: pk, SK: 'VERSION#latest' } })
  return false if resp.nil? || resp.item.nil?

  # Note this is testing against the raw Dynamo item not API output so no top lvl 'dmp'
  related = resp.item.fetch('dmproadmap_related_identifiers', [])
  related.any? && !related.first['citation'].nil?
end

# Verify that the DynamoDB Streams kicked off the DmpIndexer Lambda
def verify_indices(key:)
  resp = @dynamo.get_item({ table_name: @index_table, key: })
  pk = @dmp_id.gsub('https://', 'DMP#')

  !resp.nil? && !resp.item.nil? &&
    resp.item.fetch('dmps', []).select { |i| i['pk'] == pk }.any?
end

# Clean all records from the table for the specified PK
def clean_table(table_name:, pk:)
  resp = @dynamo.query({
    table_name:,
    key_condition_expression: 'PK = :pk',
    expression_attribute_values: { ':pk': pk },
    projection_expression: 'PK, SK'
  })
  items = resp.items.is_a?(Array) ? resp.items : [resp.items]
  items.compact.each do |item|
    key = { PK: pk, SK: item['SK'] }
    @dynamo.delete_item({ table_name:, key: })
    puts Rainbow("        Removed item #{key} from #{table_name}").bg(:green).black
  end
end

# Remove the PK from the index or delete the index if the PK is the only DMP
def clean_index(dmp_pk:, key:)
  resp = @dynamo.get_item({ table_name: @index_table, key: })
  return false if resp.nil? || resp.item.nil?

  cleansed = resp.item.fetch('dmps', []).reject { |entry| entry['pk'] == dmp_pk }
  @dynamo.delete_item({ table_name: @index_table, key: }) if cleansed.empty?
  puts Rainbow("        Removed item #{key} from #{@index_table}").bg(:green).black if cleansed.empty?
  return true if cleansed.empty?

  updated = resp.item
  updated['dmps'] = cleansed
  @dynamo.put_item({ table_name: @index_table, item: updated })
  puts Rainbow("        Updated item #{key} in #{@index_table} to remove test DMP").bg(:green).black
end

# Cleanup all of the test records from Dynamo
def cleanup
  if @dmp_id
    puts Rainbow("Cleaning up test records ...").bg(:green)
    pk = @dmp_id.gsub('https://', 'DMP#')
    # Delete all versions of the DMP
    clean_table(table_name: @dmp_table, pk:)
    # Delete the indices for the DMP
    clean_table(table_name: @index_table, pk:)
    # Remove the DMP from the person index
    clean_index(key: { PK: 'PERSON_INDEX', SK: @email }, dmp_pk: pk)
    clean_index(key: { PK: 'PERSON_INDEX', SK: @orcid }, dmp_pk: pk)
    # Remove the DMP from the affiliation index
    clean_index(key: { PK: 'AFFILIATION_INDEX', SK: @org }, dmp_pk: pk)
    # Remove the DMP from the funder index
    clean_index(key: { PK: 'FUNDER_INDEX', SK: @funder }, dmp_pk: pk)
    # Remove the contributor ORCID index
    clean_index(key: { PK: 'PERSON_INDEX', SK: 'https://orcid.org/9999-9999-9999-9999' }, dmp_pk: pk)
  end
end

# Helper method to output the response of a test
def test_it(passed: false, msg: 'was set')
  puts passed ? Rainbow("       #{msg} - YES").green : Rainbow("       #{msg} - NO").red
end

@email = 'tester@example.com'
@orcid = 'https://orcid.org/9999-9999-9999-999X'
@org = 'https://ror.org/03yrm5c26'
@funder = 'https://ror.org/052csg198'

@dmp = {
  dmp: {
    title: 'Integration testing of the DMPHub API',
    description: 'This is just a test',
    modified: '2022-11-14T22:18:18Z',
    created:'2021-11-08T19:06:04Z',
    contact: {
      name: 'Example Tester',
      dmproadmap_affiliation: {
        name: 'California Digital Library (cdlib.org)',
        affiliation_id: {
          type: 'ror',
          identifier: @org
        }
      },
      mbox: @email,
      contact_id:{
        type: 'orcid',
        identifier: @orcid
      }
    },
    dmp_id: {
      identifier: 'https://dmptool.org/plans/abcdefg_TEST',
      type: 'url'
    },
    project: [
      {
        title: 'Example Research Project',
        funding: [
          {
            name: 'Alfred P. Sloan Foundation',
            funder_id: {
              type: 'ror',
              identifier: @funder
            },
            funding_status: 'applied',
            dmproadmap_opportunity_number: 'ABC123'
          }
        ]
      }
    ],
    dataset: [
      {
        title: 'Integration testing of the DMPHub API'
      }
    ],
    dmproadmap_related_identifiers: [
      descriptor: 'references',
      work_type: 'output_management_plan',
      type: 'doi',
      identifier: 'https://doi.org/10.48321/D17598' # FAIR Island DMP
    ]
  }
}

if ARGV.length == 3
  @default_region = 'us-west-2'

  @env = ARGV[0]
  @client_id = ARGV[1]
  @client_secret = ARGV[2]

  @prefix = "uc3-dmp-hub-#{@env}"
  @stack_exports = fetch_cf_stack_exports
  @dmp_table = fetch_cf_output(name: 'DynamoTableName')
  @index_table = fetch_cf_output(name: 'DynamoIndexTableName')

  @domain = "dmphub.uc3#{@env}.cdlib.net"
  @dynamo = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

  if @env.downcase == 'prd'
    # We need a way to skip EZID for the tests in prod
    puts Rainbow("Testing against prd is not currently available!").red
  else
    begin
      # Authenticate
      auth
      if @token
        puts Rainbow("  authentication succeeded.").cyan
        @options = {
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': "Bearer #{@token}"
          },
          follow_redirects: true,
          limit: 3,
        }
        puts ""

        # Validate the DMP
        if validate_dmp
          puts Rainbow("    validation succeeded").cyan
          puts ""
          # Create the DMP
          created = create_dmp

          if created && created.fetch('dmp', {}).fetch('dmp_id', {})['identifier']
            @dmp_id = created['dmp']['dmp_id']['identifier']
            puts Rainbow("    created #{@dmp_id}").cyan
            test_it(
              passed: !created['dmp']['created'].nil?,
              msg: 'created timestamp was set'
            )
            test_it(
              passed: !created['dmp']['modified'].nil?,
              msg: 'modified timestamp was set'
            )
            test_it(
              passed: !created['dmp']['registered'].nil?,
              msg: 'registered timestamp was set'
            )
            test_it(
              passed: created['dmp'].fetch('dmphub_modifications', []).empty?,
              msg: 'dmphub_modifications was empty'
            )
            test_it(
              passed: created['dmp'].fetch('dmphub_versions', []).empty?,
              msg: 'dmphub_versions was empty'
            )
            test_it(
              passed: created['dmp']['dmproadmap_featured'] == '0',
              msg: 'dmproadmap_featured was 0'
            )
            puts ""

            # Pause and allow Citer, DmpIndexer and EzidPublisher to run
            puts Rainbow("    Giving DyanmoStream 5 secs to finish").yellow
            puts ""
            sleep(5)
            puts Rainbow("    checking indices").cyan
            test_it(
              passed: verify_indices(key: { PK: 'PERSON_INDEX', SK: @email }),
              msg: 'Email was properly indexed'
            )
            test_it(
              passed: verify_indices(key: { PK: 'PERSON_INDEX', SK: @orcid }),
              msg: 'ORCID was properly indexed'
            )
            test_it(
              passed: verify_indices(key: { PK: 'AFFILIATION_INDEX', SK: @org }),
              msg: 'Affiliation was properly indexed'
            )
            test_it(
              passed: verify_indices(key: { PK: 'FUNDER_INDEX', SK: @funder }),
              msg: 'Funder was properly indexed'
            )

            puts ""
            puts Rainbow("    Giving Citer and EZID Publisher another 5 secs to finish").yellow
            puts ""
            sleep(5)

            puts Rainbow("    checking citation").cyan
            test_it(
              passed: verify_citation,
              msg: 'Citation is present on dmproadmap_related_identifier'
            )
            puts ""

            # TODO: Check that the Ezid publisher was called

            # Update the DMP by adding a contributor
            old_mod_tstamp = created['dmp']['modified']
            @dmp = created.dup
            @dmp['dmp']['contributor'] = [{
              name: 'Example Tester 2',
              role: ['http://credit.niso.org/contributor-roles/investigation'],
              dmproadmap_affiliation: {
                name: 'California Digital Library (cdlib.org)',
                affiliation_id: {
                  type: 'ror',
                  identifier: 'https://ror.org/03yrm5c26'
                }
              },
              contributor_id: {
                type: 'orcid',
                identifier: 'https://orcid.org/9999-9999-9999-9999'
              }
            }]

            updated = update_dmp
            puts Rainbow("    updated #{@dmp_id}").cyan
            test_it(
              passed: updated['dmp']['modified'] && updated['dmp']['modified'] > old_mod_tstamp,
              msg: 'modified timestamp was updated'
            )
            test_it(
              passed: updated['dmp'].fetch('dmphub_versions', []).empty?,
              msg: 'was not versioned because it is too soon'
            )
            test_it(
              passed: updated['dmp'].fetch('contributor', [{}]).first['name'] == 'Example Tester 2',
              msg: 'saved our contributor change'
            )
            old_mod_tstamp = updated['dmp'].fetch('modified', old_mod_tstamp)
            puts ""

            # Pause to allow the DmpIndexer and EzidPublisher to run
            puts Rainbow("    Giving DyanmoStream 5 secs to finish").yellow
            puts ""
            sleep(5)
            puts Rainbow("    checking indices").cyan
            test_it(
              passed: verify_indices(key: { PK: 'PERSON_INDEX', SK: 'https://orcid.org/9999-9999-9999-9999' }),
              msg: 'ORCID was properly indexed'
            )
            puts ""

            # Tombstone the DMP
            tombstoned = tombstone_dmp
            puts Rainbow("    tombstoned #{@dmp_id}").cyan

            test_it(
              passed: tombstoned['dmp']['modified'] && tombstoned['dmp']['modified'] > old_mod_tstamp,
              msg: 'modified timestamp was updated'
            )
            test_it(
              passed: tombstoned['dmp']['title'].start_with?('OBSOLETE: '),
              msg: 'title was prefixed with `OBSOLETE: ` messaging'
            )

            # cleanup the DynamoTables
            puts ""
            puts ""
            cleanup
          else
            puts Rainbow("     unable to create the DMP!").red
          end
        else
          puts Rainbow("    validation failed. Skipping remaining tests!").red
        end
      else
        puts Rainbow("Unable to fetch an auth Token. Skipping remaining tests!").red
      end
    rescue StandardError => e
      puts ""
      puts ""
      puts Rainbow("FATAL EROR: #{e.message}").red
      puts Rainbow(e.backtrace).red
      # cleanup the DynamoTables
      puts ""
      puts ""
      cleanup
    end
  end
else
  puts "Wrong args! Expected env, Cognito client id, Cognito client secret"
end