# frozen_string_literal: true

require 'aws-sdk-cloudformation'
require 'aws-sdk-dynamodb'

# Fetches all of the CloudFormation Stack Outputs (they must have been 'Exported')
def fetch_cf_stack_exports
  exports = []
  cf_client = Aws::CloudFormation::Client.new(region: 'us-west-2')
  exports << cf_client.list_exports.exports
  exports.flatten
end

# Search the stack outputs for the name
def fetch_cf_output(name:)
  vals = @stack_exports.select do |exp|
    (name&.downcase&.strip == 'lambdasecuritygroupid' && exp.name.downcase.strip == 'lambdasecuritygroupid') ||
    ((exp.exporting_stack_id.include?(@prefix) || exp.exporting_stack_id.include?("uc3-#{@env}") ) &&
      "#{@env}-#{name&.downcase&.strip}" == exp.name.downcase.strip)
  end
  vals&.first&.value
end

# Recursive function that goes and fetches every unique PK from the Dynamo table
def fetch_indices(client:, items: [], last_key: '')
  args = {
    table_name: @index_table,
    consistent_read: false,
    projection_expression: 'PK, SK',
    key_condition_expression: 'PK = :pk',
    expression_attribute_values: { ':pk': @resource_type }
  }
  args[:exclusive_start_key] = last_key unless last_key == ''
  resp = client.query(args)

  # p "Scanning - Item Count: #{resp.count}, Last Key: #{resp.last_evaluated_key}"
  items += resp.items
  return fetch_indices(client:, items:, last_key: resp.last_evaluated_key) unless resp.last_evaluated_key.nil?

  items
end

# Recursive function that goes and fetches every unique PK from the Dynamo table
def fetch_recs(client:, items: [], last_key: '')
  args = {
    table_name: @external_data_table,
    consistent_read: false,
    projection_expression: 'ID',
    expression_attribute_values: { ':resource_type': @resource_type },
    filter_expression: 'RESOURCE_TYPE = :resource_type'
  }
  args[:exclusive_start_key] = last_key unless last_key == ''
  resp = client.scan(args)

  # p "Scanning - Item Count: #{resp.count}, Last Key: #{resp.last_evaluated_key}"
  items += resp.items
  return fetch_recs(client:, items:, last_key: resp.last_evaluated_key) unless resp.last_evaluated_key.nil?

  items
end


if ARGV.length >= 2
  @env = ARGV[0]
  @resource_type = ARGV[1]

  @prefix = "uc3-dmp-hub-#{@env}"
  @stack_exports = fetch_cf_stack_exports
  @external_data_table = fetch_cf_output(name: 'ExternalDataDynamoTableArn')
  @index_table = fetch_cf_output(name: 'DynamoIndexTableName')

  dynamo = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

  # Fetch all of the existing index records and delete them
  indices = fetch_indices(client: dynamo)
  puts "Detected #{indices.length} index records. Clearing them ..."
  indices.each do |idx|
    dynamo.delete_item(table_name: @index_table, key: { PK: idx['PK'], SK: idx['SK'] })
  end

  # Fetch all of the DMP ID records
  items = fetch_recs(client: dynamo)
  puts "Found #{items.length} unique #{@resource_type} records. Updating the records to trigger the stream ...."
  cntr = 0
  items.each do |item|
    # Uncomment and update the ROR here if you just want to fix one index record
    # next unless item['ID'] == 'https://ror.org/000x2q781'

    # Fetch the full record
    resp = dynamo.get_item({
      table_name: @external_data_table,
      key: {  RESOURCE_TYPE: @resource_type, ID: item['ID'] },
      consistent_read: false
    })
    entry = resp[:item].is_a?(Array) ? resp[:item].first : resp[:item]
    puts "Couldn't load the full record for #{item}!" if entry.nil?
    next if entry.nil?

    # Update an internal field that will trigger the dynamo stream update without altering any of the
    # true DMP-ID fields
    entry['dmphub_forced_index_recreation_date'] = Time.now.strftime('%Y-%m-%dT%H:%M')
    dynamo.put_item({
      table_name: @external_data_table,
      item: entry
    })
    puts "    updated #{item['ID']} - #{entry['name']}"
    cntr += 1
  end

  puts "Done. Updated the index for #{cntr} #{@resource_type}."
else
  puts "Expected 2 arguments, the environment and the RESOURCE_TYPE!"
  puts "    e.g. ruby recreate_external_data_index.rb dev AFFILIATION"
end
