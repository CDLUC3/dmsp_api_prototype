# frozen_string_literal: true

require 'aws-sdk-dynamodb'

# Recursive function that goes and fetches every unique PK from the Dynamo table
# that has associated HARVESTER_MODS
def fetch_dmp_ids(client:, table:, items: [], last_key: '')
  args = {
    table_name: table,
    consistent_read: false,
    projection_expression: 'PK',
    expression_attribute_values: { ':version': 'HARVESTER_MODS' },
    filter_expression: 'SK = :version'
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
  output = {}

  items.each do |item|
    # Useful debug line to isolate a specific DMP
    # next unless item['PK'] == 'DMP#doi.org/10.48321/D1CW23'

    # Fetch the full record
    resp = dynamo.get_item({
      table_name: table,
      key: {  PK: item['PK'], SK: 'HARVESTER_MODS' },
      consistent_read: false
    })
    dmp = resp[:item].is_a?(Array) ? resp[:item].first : resp[:item]
    puts "Couldn't load the full record for #{item}!" if dmp.nil?
    next if dmp.nil?

    dmp_id = dmp['PK'].gsub('PK#', 'https://')
    output[dmp_id] = [] if output[dmp_id].nil?

    works = dmp.fetch('related_works', {})
    works.keys.each do |id|
      next if output[dmp_id].select { |entry| entry[:id] == id }.any?

      output[dmp_id] << {
        id: id,
        work_type: works[id]['work_type'],
        score: works[id]['score'],
        confidence: works[id]['confidence'],
        logic: works[id]['logic'],
        provenance: works[id]['provenance'],
        citation: works[id]['citation'],
        secondary_works: works[id]['secondary_works']
      }
    end

    cntr += 1
  end

  file_path = "./tmp"
  FileUtils.mkdir_p file_path

  file_name = "#{file_path}/harvester_mods_#{Time.now.strftime('%Y_%m_%d-%H%M')}.json"
  file = File.open(file_name, 'w+')
  file.write(output.to_json)

  puts "Done. Found harvested works for #{cntr} DMP-IDs. Output written to #{file_name}"
else
  puts "Expected 2 arguments, the environment and the DynamoTable name!"
end
