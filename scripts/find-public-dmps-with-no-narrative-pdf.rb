# frozen_string_literal: true

require 'aws-sdk-dynamodb'

# Recursive function that goes and fetches every unique PK from the Dynamo table
# that has associated HARVESTER_MODS
def fetch_dmp_ids(client:, table:, items: [], last_key: '')
  args = {
    table_name: table,
    consistent_read: false,
    expression_attribute_values: { ':sk': 'VERSION#latest' },
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
  puts "Found #{items.length} unique DMP-IDs."

  # Find all the public DMPs that have no PDF narrative
  pub = items.select { |item| item['visibility'] == 'public' }
  orphaned = pub.select do |item|
    r_ids = item['dmproadmap_related_identifiers']&.select do |r_id|
      r_id['descriptor'] == 'is_metadata_for' && r_id['work_type'] == 'output_management_plan'
    end

    r_ids&.length < 1
  end

  if orphaned.length > 0
    puts "The following public DMPs do not have a PDF narrative!"
    orphaned.each { |d| puts "  #{d['PK']}"}
  else
    puts "All public DMPs have a PDF narrative document"
  end

else
  puts "Expected 2 arguments, the environment and the Dynamo Index Table name!"
end
