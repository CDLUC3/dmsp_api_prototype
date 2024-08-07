# frozen_string_literal: true

require 'aws-sdk-cloudformation'
require 'aws-sdk-dynamodb'
require 'uc3-dmp-citation'

# Fetches all of the CloudFormation Stack Outputs (they must have been 'Exported')
def fetch_cf_stack_exports
  exports = []
  cf_client = Aws::CloudFormation::Client.new(region: 'us-west-2')
  exports << cf_client.list_exports.exports
  exports.flatten
end

# Search the stack outputs for the name
def fetch_cf_output(name:)
  vals = @stack_exports.select { |exp| "#{@env}-#{name&.downcase&.strip}" == exp.name.downcase.strip }
  vals&.first&.value
end

# Recursive function that goes and fetches every unique PK from the Dynamo table
def fetch_dmp_ids(client:, items: [], last_key: '')
  exists_filter = '(attribute_exists(dmproadmap_related_identifiers) AND size(dmproadmap_related_identifiers) > :size)'

  args = {
    table_name: @table,
    consistent_read: false,
    projection_expression: 'PK, dmproadmap_related_identifiers',
    expression_attribute_values: { ':version': 'VERSION#latest', ':size': 0 },
    filter_expression: "SK = :version AND #{exists_filter}"
  }
  args[:exclusive_start_key] = last_key unless last_key == ''
  resp = client.scan(args)

  # p "Scanning - Item Count: #{resp.count}, Last Key: #{resp.last_evaluated_key}"
  items += resp.items
  return fetch_dmp_ids(client:, items:, last_key: resp.last_evaluated_key) unless resp.last_evaluated_key.nil?

  items
end

if ARGV.length == 1
  @env = ARGV[0]
  @stack_exports = fetch_cf_stack_exports
  @table = fetch_cf_output(name: 'DynamoTableName')

  client = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

  # Fetch all of the DMP ID records
  items = fetch_dmp_ids(client:)
  puts "Found #{items.length} unique DMP-IDs."

  cntr = 0
  items.each do |item|
    relateds = item.fetch('dmproadmap_related_identifiers', [])
    next if relateds.empty? # this should never happen but check anyway

    puts "Checking DMP: #{item['PK']}"
    updated = false
    updated_relateds = []

    updated_relateds = relateds.map do |id|
      work_type = id['work_type']&.strip

      if id['type'] == 'doi' && work_type != 'output_management_plan' && id['citation'].to_s.strip.empty?
        #remove space and other characters that are not displayed and remove all white spaces
        doi = id['identifier'].gsub(/[^\x20-\x7E]/, '').gsub(/\s/, '')
        puts "  Identifier to cite: #{doi}"
        begin
          resp = Uc3DmpCitation::Citer.fetch_citation(doi:, work_type:)
        rescue StandardError => e
          puts "    ERROR from citer: #{e.message}"
          resp = nil
        end

        # Update the citation in the related_identifier
        id['citation'] = resp.nil? || resp.blank? ? 'No citation available.' : resp
        puts "    New citation: #{id['citation']}"

        updated = true
        cntr += 1
      end
      id
    end

    if updated
      # Fetch the full record
      resp = client.get_item({
        table_name: @table,
        key: {  PK: item['PK'], SK: 'VERSION#latest' },
        consistent_read: false
      })
      dmp = resp[:item].is_a?(Array) ? resp[:item].first : resp[:item]
      puts "Couldn't load the full record for #{item}!" if dmp.nil?
      next if dmp.nil?

      puts "  Updating DMP citations for #{item['PK']}"
      dmp['dmproadmap_related_identifiers'] = updated_relateds
      client.put_item({ table_name: @table, item: dmp })
    else
      puts "  No citations to fetch."
    end
  end
  puts "Done. Updated the citations for #{cntr} related identifiers."
else
  puts "Expected 1 argument, the environment!"
end
