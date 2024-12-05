require 'aws-sdk-dynamodb'
require 'aws-sdk-eventbridge'

if ARGV.length >= 2
  env = ARGV[0]
  bus_arn = ARGV[1]

  bridge = Aws::EventBridge::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))
  message = {
    entries: [{
      time: Time.now.utc.iso8601,
      source: "dmphub.uc3#{env}.cdlib.net:lambda:event_publisher",
      detail_type: "ScheduleDmpExtraction",
      detail: '{}',
      event_bus_name: bus_arn
    }]
  }

  puts "Sending a message to the EventBus to kick off the DMP Extractor function"
  pp message

  resp = bridge.put_events(message)

  puts "Done."
else
  puts "Expected 2 arguments, the environment and EventBus ARN!"
end
