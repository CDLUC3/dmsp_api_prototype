# frozen_string_literal: true

require 'aws-sdk-dynamodb'

# Recursive function that goes and fetches every unique PK from the Dynamo table
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
  puts "Found #{items.length} unique DMP-IDs. Updating the index ...."
  cntr = 0

  affiliations = [
    "https://ror.org/0168r3w48", # University of California, San Diego (ucsd.edu)
    "https://ror.org/01an7q238", # University of California, Berkeley (berkeley.edu)
    "https://ror.org/03nawhv43", # University of California, Riverside (ucr.edu)
    "https://ror.org/02t274463", # University of California, Santa Barbara (ucsb.edu)
    "https://ror.org/03efmqc40", # Arizona State University (asu.edu)
    "https://ror.org/000e0be47", # Northwestern University (northwestern.edu)
    "https://ror.org/04p491231", # Pennsylvania State University (psu.edu)
    "https://ror.org/00f54p054", # Stanford University (stanford.edu)
    "https://ror.org/02ttsq026", # University of Colorado Boulder (colorado.edu)
    "https://ror.org/005dvqh91", # New York University Langone Medical Center (nyulangone.org)
  ]
  json = []

  now = DateTime.now
  file_name = "DMP-INDEX-#{now.strftime('%Y-%m-%d')}.json"
  file = File.new(file_name, 'w+');
  File.open(file_name, 'w') do |file|
    items.each do |item|
      next unless item['affiliation_ids'].select { |id| affiliations.include?(id) }.any?

      file.puts(item.to_json)
      cntr += 1
    end
  end

  puts "Done. Dumped the index for #{cntr} DMP-IDs."
else
  puts "Expected 2 arguments, the environment and the DynamoTable name!"
end
