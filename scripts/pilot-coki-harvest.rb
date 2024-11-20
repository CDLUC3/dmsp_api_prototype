require 'csv'
require 'jsonl'

pilot_orgs = {
  "https://ror.org/03nawhv43": {
    "name": "University of California, Riverside",
    "pilot_dmps": [
      "https://doi.org/10.48321/D106FD79D7",
      "https://doi.org/10.48321/D117F9DC89",
      "https://doi.org/10.48321/D11E48BC8B",
      "https://doi.org/10.48321/D13BEA529C",
      "https://doi.org/10.48321/D14406894e",
      "https://doi.org/10.48321/D145457051",
      "https://doi.org/10.48321/D1781C1ACE",
      "https://doi.org/10.48321/D184612CA9",
      "https://doi.org/10.48321/D1AD94C04A",
      "https://doi.org/10.48321/D1C7466457",
      "https://doi.org/10.48321/D1FCB77AF0",
      "https://doi.org/10.48321/D1FFBFF8FE"
    ],
  },
  "https://ror.org/03efmqc40": {
    "name": "Arizona State University",
    "pilot_dmps": ["https://doi.org/10.48321/D190AA982F"]
  },
  "https://ror.org/02ttsq026": {
    "name": "University of Colorado Boulder",
    "pilot_dmps": [
      "https://doi.org/10.48321/D12D3B625A",
      "https://doi.org/10.48321/D14F38aa13",
      "https://doi.org/10.48321/D1B581751F"
    ]
  },
  "https://ror.org/02t274463": {
    "name": "University of California, Santa Barbara",
    "pilot_dmps": [
      "https://doi.org/10.48321/D154FA23E9",
      "https://doi.org/10.48321/D182FD4E77",
      "https://doi.org/10.48321/D1876B22F8",
      "https://doi.org/10.48321/D1A90CCC2B",
      "https://doi.org/10.48321/D1B37E20FE",
      "https://doi.org/10.48321/D1BAD5B94D",
      "https://doi.org/10.48321/D1FFE5D7FD"
    ]
  },
  "https://ror.org/01an7q238": {
    "name": "University of California, Berkeley",
    "pilot_dmps": [
      "https://doi.org/10.48321/D114471AC3",
      "https://doi.org/10.48321/D11FB0F3A2",
      "https://doi.org/10.48321/D1425BBD9F",
      "https://doi.org/10.48321/D16BBE57E6",
      "https://doi.org/10.48321/D18F9B93B8",
      "https://doi.org/10.48321/D19D861916",
      "https://doi.org/10.48321/D1AED95FF4",
      "https://doi.org/10.48321/D1BA48FBC9",
      "https://doi.org/10.48321/D1CE350633",
      "https://doi.org/10.48321/D1DF9DDDAF"
    ]
  },
  "https://ror.org/0168r3w48": {
    "name": "University of California, San Diego",
    "pilot_dmps": ["https://doi.org/10.48321/D11628EB15"]
  },
  "https://ror.org/000e0be47": {
    "name": "Northwestern University",
    "pilot_dmps": [
      "https://doi.org/10.48321/D10B3E54E4",
      "https://doi.org/10.48321/D10CA0DB07",
      "https://doi.org/10.48321/D1143FD15F",
      "https://doi.org/10.48321/D139D84658",
      "https://doi.org/10.48321/D1841AE523",
      "https://doi.org/10.48321/D1944C8215",
      "https://doi.org/10.48321/D1A04A9B1D",
      "https://doi.org/10.48321/D1A3A05164",
      "https://doi.org/10.48321/D1B7947E97",
      "https://doi.org/10.48321/D1C3553D75"
    ]
  }
}

pilot_rors = pilot_orgs.keys.map { |key| key.to_s.gsub('https://ror.org/', '') }
pilot_dmps = pilot_orgs.values.map { |v| v[:pilot_dmps] }.flatten.uniq
pilot_dmps = pilot_dmps.map { |dmp| dmp.gsub('https://', 'DMP#') }

puts "Processing COKI harvester files"
works = []
works_csv = [
  ['affiliation_id', 'affiliation_name', 'dmp_id', 'is_pilot_dmp', 'provenance',
   'related_work', 'type',
   'publisher', 'container', 'publication_date',
   'title', 'location', 'authors',
   'weighted_score']
]
relevant = []

# Loop through the match files
Dir.children("#{Dir.pwd}/tmp/coki/").each do |file|
  next if file.start_with?('matches')

  puts "Searching for relevant matches in #{file} ..."

  source = File.read("#{Dir.pwd}/tmp/coki/#{file}")
  parsed = JSONL.parse(source)

  # Find the relevant records. Either pilot DMP IDs uploaded via the UI or ones that are
  # affiliated with one of the pilot partner RORs
  items = parsed.select do |line|
    entry_affils = line.fetch("dmp", {}).fetch("affiliation_ids", [])
    pilot_dmps.include?(line["dmp_doi"]) || (pilot_rors & entry_affils).any?
  end

  relevant << items.map do |item|
    item["provenance"] = file.split('-').first
    item
  end
end

puts "Found #{relevant.length} potentially relevant matches."
puts "Processing relevant matches ..."
# Process the relevant records
relevant.flatten.uniq.each do |line|
  entry_matches = line.fetch("matches", [])
  entry_affils = line.fetch("dmp", {}).fetch("affiliation_ids", [])
  next unless entry_matches.any? && entry_affils.any?

  dmp_id = line["dmp_doi"].gsub('DMP#', 'https://')
  pilot_dmp = pilot_dmps.include?(line["dmp_doi"])

  # Process each potential match
  entry_matches.each do |match|
    count = match.fetch("counts", {})["weighted_count"].to_f
    # Only allow matches whose weight is less than 10
    next if count.nil? || count < 10 || match.fetch("biblio_data", {})["title"].nil?

    biblio_data = match.fetch("biblio_data", {})

    location = biblio_data["volume"].nil? ? [] : ["vol: #{biblio_data["volume"]}"]
    location << "iss: #{biblio_data["issue"]}" unless biblio_data["issue"].nil?
    location << "pgs: #{biblio_data["page"]}" unless biblio_data["page"].nil?

    match_affils = match.fetch("match_data", {}).fetch("affiliation_ids", [])
    affils = entry_affils & match_affils

    affils.each do |affil|
      affil_name = pilot_orgs.fetch(:"https://ror.org/#{affil}", {})[:name]
      next if affil_name.nil?

      works << {
        affiliation_id: affil,
        affiliation_name: affil_name,
        dmp_id: dmp_id,
        is_pilot_dmp: pilot_dmp,
        provenance: line["provenance"],
        related_work: "https://doi.org/#{match["match_doi"]}",
        type: biblio_data["type"],
        container: biblio_data["container_title"],
        publisher: biblio_data["publisher"],
        publication_date: biblio_data["publication_date"],
        title: biblio_data["title"],
        location: location.join(', '),
        authors: biblio_data.fetch("authors", []).map { |e| [e["given"], e["family"]].join(' ') }.join(' | '),
        weighted_score: count
      }

      works_csv << [
        affil, affil_name, dmp_id, pilot_dmp, line["provenance"],
        "https://doi.org/#{match["match_doi"]}", biblio_data["type"],
        biblio_data["publisher"], biblio_data["container_title"], biblio_data["publication_date"],
        biblio_data["title"], location.join(', '),
        biblio_data.fetch("authors", []).map { |e| [e["given"], e["family"]].join(' ') }.join(' | '),
        count
      ]
    end
  end
end

file = File.open("#{Dir.pwd}/tmp/coki/matches#{Time.now.strftime('%Y-%m-%d')}.json", 'w+')
works.each { |work| file.write("#{work.to_json}\n") }
file.close

csv = File.open("#{Dir.pwd}/tmp/coki/matches#{Time.now.strftime('%Y-%m-%d')}.csv", 'w+')
works_csv.each { |work| csv.write(work.to_csv) }
csv.close
