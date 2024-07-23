# frozen_string_literal: true

require 'aws-sdk-dynamodb'
require 'uc3-dmp-citation'

# Recursive function that goes and fetches every unique PK from the Dynamo table
def fetch_dmp_ids(client:, table:, items: [], last_key: '')
  args = {
    table_name: table,
    consistent_read: false,
    projection_expression: 'PK',
    expression_attribute_values: { ':version': 'VERSION#latest' },
    filter_expression: 'SK = :version'
  }
  args[:exclusive_start_key] = last_key unless last_key == ''
  resp = client.scan(args)

  # p "Scanning - Item Count: #{resp.count}, Last Key: #{resp.last_evaluated_key}"
  items += resp.items
  return fetch_dmp_ids(client:, table:, items:, last_key: resp.last_evaluated_key) unless resp.last_evaluated_key.nil?

  items
end

if ARGV.length >= 1
  table = ARGV[0]

    dynamo = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

    # Fetch all of the DMP ID records
    items = fetch_dmp_ids(client: dynamo, table: table)
    puts "Found #{items.length} unique DMP-IDs."
    cntr = 0

    items.each do |item|

        # Fetch the full record
        resp = dynamo.get_item({
        table_name: table,
        key: {  PK: item['PK'], SK: 'VERSION#latest' },
        consistent_read: false
        })
        dmp = resp[:item].is_a?(Array) ? resp[:item].first : resp[:item]
        puts "Couldn't load the full record for #{item}!" if dmp.nil?
        next if dmp.nil?

        # Iterate over dmproadmap_related_identifiers and add citation if missing

        # Extract the dmproadmap_related_identifiers
        dmproadmap_related_identifiers = dmp['dmproadmap_related_identifiers']

            if(dmproadmap_related_identifiers)

                updated = false

                updated_related_identifiers = dmproadmap_related_identifiers.each do |related_identifier|

                    type = related_identifier['type']
                    citation = related_identifier['citation']
                    work_type = related_identifier['work_type']
            
                    if type == 'doi' && citation.to_s.strip.empty?
                        # Extract the dmp_id identifier
                        dmp_identifier = dmp['dmp_id']['identifier']
                          identifier = related_identifier['identifier'].gsub(/[^\x20-\x7E]/, '')#remove space and other characters that are not displayed
                          identifier = identifier.strip.squeeze(" ")  # Removes leading/trailing spaces and reduces multiple spaces to a single space
                          # Remove all blank spaces
                          identifier = identifier.gsub(' ', '')
                          
                          puts "IDENTIFIER [Citation]: #{identifier}"
                          puts "DMP IDENTIFIER: #{dmp_identifier}"
                          resp = Uc3DmpCitation::Citer.fetch_citation(doi: identifier.strip, work_type: work_type&.strip)
                          # Update the citation in the related_identifier
                          related_identifier['citation'] = resp
                          puts "CITATION: #{resp}"
                          updated = true
                    end
                    related_identifier
                end

                # Update the dmproadmap_related_identifiers in the dmp if there was an update
                if updated
                    dmp['dmproadmap_related_identifiers'] = updated_related_identifiers

                    # Save the updated dmp back to DynamoDB
                    # dynamo.put_item({
                    #     table_name: table,
                    #     item: dmp
                    # })
                    cntr += 1
                end
            end
    
    end

  puts "Done. Updated the index for #{cntr} DMP-IDs."
else
  puts "Expected 1 argument, the DynamoTable name!"
end
