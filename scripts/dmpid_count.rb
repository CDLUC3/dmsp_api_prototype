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
  puts "Found #{items.length} unique DMP-IDs."

  pub = items.select { |item| item['visibility'] == 'public' }.length
  priv = items.select { |item| item['visibility'] != 'public' }.length

  puts "    Public: #{pub}, Private: #{priv}"

  years = {}
  items.each do |item|
    year = item['created'][0..3]
    years[year] = 0 if years[year].nil?

    years[year] += 1    
  end

  puts "Breakdown by year"
  pp years

else
  puts "Expected 2 arguments, the environment and the Dynamo Index Table name!"
end

