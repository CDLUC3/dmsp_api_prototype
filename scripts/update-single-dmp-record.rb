require 'aws-sdk-dynamodb'

# This script can be used to update the content of a single record within a DynamoDB table
#   1. You should first go into the console and load the current record.
#   2. Copy the content of the record into the `new_record` variable below.
#   3. Modify the content of the `new_record` as needed.
#   4. Run the script
#
# If you are adding a missing PDF narrative you should do the following first:
#   1. Go into the DMP tool site as a super admin
#   2. Go to the plan page for the DMP in question and download its PDF (include everything)
#   3. Go to the AWS console and upload the PDF into the narratives folder of the `dmp-hub-[env]`
#      `cloudfrontbucket`
#   4. Make a note of the name of the PDF (e.g. `foo.pdf`)
#   5. Follow the steps above and add the PDF to the `dmproadmap_related_identifiers` array:
#
#       "dmproadmap_related_identifiers": [
#         {
#           "descriptor": "is_metadata_for",
#           "identifier": "https://dmphub.uc3[env].cdlib.net/narratives/foo.pdf",
#           "type": "url",
#           "work_type": "output_management_plan"
#         }
#       ]

if ARGV.length == 2
  TABLE = ARGV[0]
  PK = "DMP#doi.org/#{ARGV[1]}"

  dynamo = Aws::DynamoDB::Client.new(region: ENV.fetch('AWS_REGION', 'us-west-2'))

  # Fetch the full record
  resp = dynamo.get_item({
    table_name: TABLE,
    key: {  PK: PK, SK: 'VERSION#latest' },
    consistent_read: false
  })
  dmp = resp[:item].is_a?(Array) ? resp[:item].first : resp[:item]
  puts "Couldn't load the full record for #{PK}!" if dmp.nil?

  new_record = '{
                 "PK": "DMP#doi.org/10.48321/D1EBBDEE57",
                 "SK": "VERSION#latest",
                 "contact": {
                  "contact_id": {
                   "identifier": "https://orcid.org/0009-0005-5653-7813",
                   "type": "orcid"
                  },
                  "dmproadmap_affiliation": {
                   "affiliation_id": {
                    "identifier": "https://ror.org/00t33hh48",
                    "type": "ror"
                   },
                   "name": "The Chinese University of Hong Kong"
                  },
                  "mbox": "qinqinzhang@cuhk.edu.hk",
                  "name": "Qinqin Zhang"
                 },
                 "created": "2025-07-07T02:13:09Z",
                 "dataset": [
                  {
                   "description": "No individual datasets have been defined for this DMP.",
                   "title": "Generic dataset",
                   "type": "dataset"
                  }
                 ],
                 "description": "",
                 "dmphub_forced_index_recreation_date": "2025-07-31T07:17",
                 "dmphub_modifications": [
                 ],
                 "dmphub_modification_day": "2025-07-22",
                 "dmphub_owner_id": "https://orcid.org/0009-0005-5653-7813",
                 "dmphub_owner_org": "https://ror.org/00t33hh48",
                 "dmphub_provenance_id": "PROVENANCE#dmptool",
                 "dmphub_provenance_identifier": "dmptool#https/api/v2/plans/140071",
                 "dmproadmap_external_system_identifier": "https://doi.org/10.48321/D1EBBDEE57",
                 "dmproadmap_featured": "0",
                 "dmproadmap_links": {
                  "download": "https://https/api/v2/plans/140071.pdf",
                  "get": "https://https/api/v2/plans/140071"
                 },
                 "dmproadmap_privacy": "public",
                 "dmproadmap_related_identifiers": [
                   {
                     "descriptor": "is_metadata_for",
                     "identifier": "https://dmphub.uc3prd.cdlib.net/narratives/testing_the_published_new_template.pdf",
                     "type": "url",
                     "work_type": "output_management_plan"
                   }
                 ],
                 "dmproadmap_template": {
                  "id": "1170699978",
                  "title": "CUHK Data Management Plan Template"
                 },
                 "dmp_id": {
                  "identifier": "https://doi.org/10.48321/D1EBBDEE57",
                  "type": "doi"
                 },
                 "ethical_issues_exist": "unknown",
                 "language": "eng",
                 "modified": "2025-07-22T06:58:25Z",
                 "project": [
                  {
                   "description": "",
                   "end": "2027-07-22T06:58:24Z",
                   "funding": [
                    {
                     "dmproadmap_funded_affiliations": [
                      {
                       "affiliation_id": {
                        "identifier": "https://ror.org/00t33hh48",
                        "type": "ror"
                       },
                       "name": "The Chinese University of Hong Kong"
                      }
                     ],
                     "funding_status": "planned",
                     "name": "No Founder"
                    }
                   ],
                   "start": "2025-07-22T06:58:24Z",
                   "title": "testing the published new template"
                  }
                 ],
                 "registered": "2025-07-17T08:39:00Z",
                 "title": "testing the published new template"
                }'

  new_dmp = JSON.parse(new_record)
  match = new_dmp[:PK] == dmp[:PK]
  puts "Record specified does not match content of the new record!" unless match

  unless dmp.nil? && match
    # Update an internal field that will trigger the dynamo stream update without altering any of the
    # true DMP-ID fields
    new_dmp['dmphub_forced_index_recreation_date'] = Time.now.strftime('%Y-%m-%dT%H:%M')
    dynamo.put_item({
      table_name: TABLE,
      item: new_dmp
    })
    puts "Done. Record updated. See cloudwatch log for details"
  end
else
  p "Expected 2 arguments, the Dynamo table name and the DMP ID (just the shoulder and id)"
end
