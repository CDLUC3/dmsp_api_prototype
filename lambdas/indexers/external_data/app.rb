# frozen_string_literal: true

# Docs say that the LambdaLayer gems are found mounted as /opt/ruby/gems but an inspection
# of the $LOAD_PATH shows that only /opt/ruby/lib is available. So we add what we want here
# and indicate exactly which folders contain the *.rb files
my_gem_path = Dir['/opt/ruby/gems/**/lib/']
$LOAD_PATH.unshift(*my_gem_path)

require 'opensearch-aws-sigv4'
require 'aws-sigv4'

require 'uc3-dmp-api-core'
require 'uc3-dmp-cloudwatch'
require 'uc3-dmp-dynamo'
require 'uc3-dmp-id'

module Functions
  # A service that indexes ROR ids into OpenSearch
  class ExternalDataIndexer
    # SOURCE = 'DMP-ID Dynamo Table Stream to OpenSearch'
    SOURCE = 'External Data Dynamo Table Stream to Dynamo Index'

    # Parameters
    # ----------
    # event: Hash, required
    #     DynamoDB Stream Event Input:
    #       {
    #         "eventID": "53041a9383eb551d8e1d5cc062aa7ebd",
    #         "eventName": "MODIFY",
    #         "eventVersion": "1.1",
    #         "eventSource": "aws:dynamodb",
    #         "awsRegion": "us-west-2",
    #         "dynamodb": {
    #           "ApproximateCreationDateTime": 1698878479.0,
    #           "Keys": {
    #             "ID": { "S": "https://ror.org/04p8xrf95" },
    #             "RESOURCE_TYPE": { "S": "AFFILIATION" }
    #           },
    #           "NewImage": {
    #             "id": { "S": "https://ror.org/04p8xrf95" },
    #             "name": { "S": "Tetiaroa Society" },
    #             "established":2010,
    #             "types": { "L": { "S": "Nonprofit" } },
    #             "links": { "L": { "S": "https://www.tetiaroasociety.org/" } },
    #             "acronyms": { "L": { "S": "TS" } },
    #             "status": { "S": "active" },
    #             "country": {
    #               "M": {
    #                 "country_name": { "S": "French Polynesia" },
    #                 "country_code": { "S": "PF" }
    #               }
    #             },
    #             "RESOURCE_TYPE": { "S": "AFFILIATION" },
    #             "ID": { "S": "https://ror.org/04p8xrf95" }
    #           },
    #           "SequenceNumber": "1157980700000000064369222776",
    #           "SizeBytes": 206,
    #           "StreamViewType": "NEW_IMAGE"
    #         },
    #         "eventSourceARN": "arn:aws:dynamodb:us-west-2:MY_ACCT:table/TABLE_ID/stream/2023-11-01T20:51:23.151"
    #       }
    #
    # context: object, required
    #     Lambda Context runtime methods and attributes
    #     Context doc: https://docs.aws.amazon.com/lambda/latest/dg/ruby-context.html
    class << self
      def process(event:, context:)
        records = event.fetch('Records', [])

        log_level = ENV.fetch('LOG_LEVEL', 'error')
        req_id = context.is_a?(LambdaContext) ? context.aws_request_id : event['id']
        logger = Uc3DmpCloudwatch::Logger.new(source: SOURCE, request_id: req_id, event:, level: log_level)

        # TODO: Eventually reenable this once we have OpenSearch in a stable situation
        # client = _open_search_connect(logger:) if records.any?
        client = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))
        table = ENV['DYNAMO_INDEX_TABLE']
        record_count = 0

        records.each do |record|
          pk = record.fetch('dynamodb', {}).fetch('Keys', []).fetch('RESOURCE_TYPE', {})['S']
          sk = record.fetch('dynamodb', {}).fetch('Keys', []).fetch('ID', {})['S']
          payload = record.fetch('dynamodb', {}).fetch('NewImage', {})
          next if pk.nil? || sk.nil? || payload.nil?

          logger&.debug(message: "Processing change to DynamoDB record #{sk}", details: record)

          case record['eventName']
          when 'REMOVE'
            logger&.info(message: "Removing OpenSearch record")
          when 'MODIFY'
            logger&.info(message: "Updating OpenSearch record")

            case pk
            when 'AFFILIATION'
              doc = _ror_to_index(client:, table:, hash: payload, logger:)
            # Do something for Metadata Standards, Licenses, Repositories, etc.
            end
            # client.index(
            #   index: ENV['OPEN_SEARCH_INDEX'],
            #   body: _dmp_to_os_doc(hash: payload, logger:),
            #   id: pk,
            #   refresh: true
            # )
          else
            logger&.info(message: "Creating OpenSearch record")
            case pk
            when 'AFFILIATION'
              doc = _ror_to_index(client:, table:, hash: payload, logger:)
            # Do something for Metadata Standards, Licenses, Repositories, etc.
            end
            # client.index(
            #   index: ENV['OPEN_SEARCH_INDEX'],
            #   body: _dmp_to_os_doc(hash: payload, logger:),
            #   id: pk,
            #   refresh: true
            # )
          end

          record_count += 1
        end

        logger&.info(message: "Processed #{record_count} records.")
        "Processed #{record_count} records."
      rescue Net::OpenTimeout => e
        puts "ERROR: Unable to establish a connection to OpenSearch! #{e.message}"
        Uc3DmpApiCore::Notifier.notify_administrator(source: SOURCE, details: { message: e.message }, event:)
      rescue StandardError => e
        puts "ERROR: Updating OpenSearch index: #{e.message}"
        puts e.backtrace
        details = { message: e.message, backtrace: e.backtrace }
        Uc3DmpApiCore::Notifier.notify_administrator(source: SOURCE, details:, event:)
      end

      private

      # Establish a connection to OpenSearch
      def _open_search_connect(logger:)
        # NOTE the AWS credentials are supplied to the Lambda at Runtime, NOT passed in by CloudFormation
        signer = Aws::Sigv4::Signer.new(
          service: 'es',
          region: ENV['AWS_REGION'],
          access_key_id: ENV['AWS_ACCESS_KEY_ID'],
          secret_access_key: ENV['AWS_SECRET_ACCESS_KEY'],
          session_token: ENV['AWS_SESSION_TOKEN']
        )
        client = OpenSearch::Aws::Sigv4Client.new({ host: ENV['OPEN_SEARCH_DOMAIN'], log: true }, signer)
        logger&.debug(message: client&.info)

        # Create the index if it does not already exist
        index_exists = client.indices.exists(index: ENV['OPEN_SEARCH_INDEX'])
        logger&.info(message: "Creating index '#{ENV['OPEN_SEARCH_INDEX']}' because it does not exist") unless index_exists
        client.indices.create(index: ENV['OPEN_SEARCH_INDEX']) unless index_exists

        client
      rescue StandardError => e
        puts "ERROR: Establishing connection to OpenSearch: #{e.message}"
        puts e.backtrace
      end

      # Convert the name to lower case and remove all non alpha numeric characters
      def _name_for_search(val:)
        str = val.is_a?(Hash) ? val['S'] : val
        return '' unless str.is_a?(String) && !str.blank?

        str.to_s.downcase.gsub(/[^a-zA-Z0-9]/, '').strip
      end

      # Convert the ROR entry into an index record
      def _ror_to_index(client:, table:, hash:, logger:)
        return false unless hash.is_a?(Hash) && !hash.fetch('name', {})['S'].nil?

        status = hash.fetch('status', JSON.parse({ S: 'active' }.to_json))['S']&.downcase
        if status == 'active'
          aliases = hash.fetch('aliases', {}).fetch('L', []).map { |val| _name_for_search(val:) }
          aliases += hash.fetch('acronyms', []).fetch('L', []).map { |val| _name_for_search(val:) }
          links = hash['domain'].nil? ? [] : [hash.fetch('domain', {})['S']]
          links += hash.fetch('links', []).map do |link|
            next unless link.is_a?(Hash) && !link['S'].nil?

            link['S'].downcase.gsub(/https?:\/\//, '').gsub('www.', '')
          end
          country = hash.fetch('country', {}).fetch('M', {})

          sk = _name_for_search(val: hash['name'])
          fundref = hash.fetch('external_ids', {}).fetch('M', {}).fetch('FundRef', {})['M']
          fundref_id = fundref.fetch('preferred', {})['S'] unless fundref.nil?
          fundref_id = fundref.fetch('all', {}).fetch('L', []).first&.fetch('S', '') if !fundref.nil? && fundref_id.nil?

          # Generate the new Indexed metadata
          rec = {
            PK: 'AFFILIATION',
            SK: sk,
            name: hash['name']['S'],
            searchName: sk,
            ror_url: hash['ID']['S'],
            ror_id: hash['ID']['S'].gsub(/https?:\/\/ror.org\//, '')
          }
          rec[:aliases] = aliases.flatten.compact.uniq if aliases.reject { |a| a.blank? }.any?
          rec[:links] = links.flatten.compact.uniq if links.reject { |a| a.blank? }.any?
          rec[:country_name] = _name_for_search(val: country['country_name']),
          rec[:country_code] = _name_for_search(val: country['country_code'])
          rec[:fundref_url] = "https://api.crossref.org/funders/#{fundref_id}" unless fundref_id.nil?
          rec[:fundref_id] = fundref_id unless fundref_id.nil?
          logger&.debug(message: "Updating index for ROR #{hash['id']} - #{hash['name']}", details: rec)

          # Update/Add the metadata for the DMP
          client.put_item(item: rec, table_name: table)
        else
          logger&.info(message: "ROR #{hash['id']} is no longer active. Removing index")
          # It's not active, so delete the index entry
          client.delete_item(
            table_name: table,
            key: { PK: 'AFFILIATION', SK: sk }
          )
        end
      end
    end
  end
end