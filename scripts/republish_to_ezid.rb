require 'aws-sdk-cloudformation'
require 'aws-sdk-eventbridge'

# Republish To EZID
# ---------------------------------------------------------------------------------------
# Use this scipt to manually trigger the EzidPublisher function for the specified DMP ID
#
# Note that the DMP ID should be specified in the [shoulder][id] format (e.g. 11.9999/A1B2C3)
# ---------------------------------------------------------------------------------------

def fetch_cf_stack_exports
  cf_client = Aws::CloudFormation::Client.new(region: 'us-west-2')
  @exports = cf_client.list_exports.exports
  @exports.flatten
end

# Search the stack outputs for the name
def fetch_eventbus_arn
  vals = @exports.select do |exp|
    exp.exporting_stack_id.include?("uc3-dmp-hub-#{@env}") &&
      "#{@env}-eventbusarn" == exp.name.downcase.strip
  end
  vals.first[:value]
end

if ARGV.length >= 2
  fetch_cf_stack_exports
  @env = ARGV[0]
  dmp_id = ARGV[1]
  bus_arn = fetch_eventbus_arn
  client = Aws::EventBridge::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

  if bus_arn
    message = {
      entries: [{
        time: Time.now.utc.iso8601,
        source: "dmphub.uc3#{@env}.cdlib.net:lambda:event_publisher",
        detail_type: "EZID update",
        detail: {
          PK: "DMP#doi.org/#{dmp_id}",
          SK: "VERSION#latest",
          dmphub_provenance_id: "PROVENANCE#dmptool"
        }.to_json,
        event_bus_name: bus_arn
      }]
    }

    puts "Sending a message to the EventBus to kick off the EzidPublisher function"
    pp message

    resp = client.put_events(message)

    if resp.failed_entry_count.nil? || resp.failed_entry_count.positive?
      puts "Unable to publish message to the EventBus!"
      pp resp
    else
      puts "Done. Kicked off the EzidPublisher Lambda Function."
    end
  else
    puts "Unable to find EventBus for #{@env}"
  end
else
  puts "Expected 2 arguments: the environment and the DOI (without the protocol and domain!"
  puts "    e.g. ruby republish_to_ezid.rb dev 10.99999/A1B2C3D4"
end
