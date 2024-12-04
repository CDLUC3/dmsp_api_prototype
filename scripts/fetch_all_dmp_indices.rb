# frozen_string_literal: true

require 'aws-sdk-dynamodb'

# Recursive function that goes and fetches every unique PK from the Dynamo table
# that has associated HARVESTER_MODS
def fetch_dmp_ids(client:, table:, items: [], last_key: '')
  args = {
    table_name: table,
    consistent_read: false,
    expression_attribute_values: { ':sk': 'METADATA' },
    filter_expression: 'SK = :sk'
  }
  args[:exclusive_start_key] = last_key unless last_key == ''
  resp = client.scan(args)

  # p "Scanning - Item Count: #{resp.count}, Last Key: #{resp.last_evaluated_key}"
  items += resp.items
  return fetch_dmp_ids(client:, table:, items:, last_key: resp.last_evaluated_key) unless resp.last_evaluated_key.nil?

  items
end

if ARGV.length >= 2
  env = ARGV[0]
  table = ARGV[1]

  dynamo = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

  # Fetch all of the DMP ID records
  items = fetch_dmp_ids(client: dynamo, table: table)
  puts "Found #{items.length} unique DMP-IDs. Generating the output file ...."

  file_path = "./tmp"
  FileUtils.mkdir_p file_path

  file_name = "#{file_path}/harvestable_dmps_#{Time.now.strftime('%Y_%m_%d-%H%M')}.json"
  file = File.open(file_name, 'w+')

  cntr = 0

  items.each do |item|
    output = {}

    # Useful debug line to isolate a specific DMP
    # next unless item['PK'] == 'DMP#doi.org/10.48321/D1CW23'

    output[:dmptool_id] = item['PK']
    output[:dmp_id] = "https://#{item['dmp_id']}"
    output[:dmp_created] = item['created']
    output[:dmp_modified] = item['modified']
    output[:dmp_doi_registered] = item['registered']
    output[:dmp_visibility] = item['visibility']

    output[:title] = item['title']&.strip
    output[:abstract] = item['description']&.strip&.gsub(%r{\t\r\n}, ' ')

    output[:project_start] = item['project_start']
    output[:project_end] = item['project_end']

    output[:affiliations] = item['affiliations']
    output[:affiliation_ids] = item['affiliation_ids']

    output[:people] = item['people']
    output[:people_ids] = item['people_ids']

    output[:funders] = item['funders']
    output[:funder_ids] = item['funder_ids']
    output[:funder_opportunity_ids] = item['funder_opportunity_ids']
    output[:grant_ids] = item['grant_ids']

    output[:repositories] = item['repos']
    output[:repository_ids] = item['repo_ids']

    file.write output.to_json
    cntr += 1
  end

  file.close

  puts "Done. Found indices for #{cntr} DMP-IDs. Output written to #{file_name}"
else
  puts "Expected 2 arguments, the environment and the Dynamo Index Table name!"
end
