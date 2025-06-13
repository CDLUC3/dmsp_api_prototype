MODEL (
  name datacite_index.types,
  dialect duckdb,
  kind FULL
);

 -- Mapping table to normalise DataCite types
WITH type_map AS (
  SELECT * FROM (VALUES
    ('Audiovisual', 'audio-visual'),
    ('Award', 'other'),
    ('Book', 'book'),
    ('BookChapter', 'book-chapter'),
    ('Collection', 'collection'),
    ('ComputationalNotebook', 'software'),
    ('ConferencePaper', 'article'),
    ('ConferenceProceeding', 'other'),
    ('DataPaper', 'data-paper'),
    ('Dataset', 'dataset'),
    ('Dissertation', 'dissertation'),
    ('Event', 'event'),
    ('Film', 'audio-visual'),
    ('Image', 'image'),
    ('Instrument', 'physical-object'),
    ('InteractiveResource', 'interactive-resource'),
    ('Journal', 'other'),
    ('JournalArticle', 'article'),
    ('List of nomenclatural and taxonomic changes for the New Zealand flora.', 'other'),
    ('Model', 'model'),
    ('Other', 'other'),
    ('OutputManagementPlan', 'output-management-plan'),
    ('PeerReview', 'peer-review'),
    ('PhysicalObject', 'physical-object'),
    ('Preprint', 'preprint'),
    ('Project', 'other'),
    ('Report', 'report'),
    ('Service', 'service'),
    ('Software', 'software'),
    ('Sound', 'sound'),
    ('Standard', 'standard'),
    ('StudyRegistration', 'other'),
    ('Text', 'text'),
    ('Workflow', 'workflow')
    ) AS t(original_type, normalized_type)
)

SELECT
  doi,
  type_map.normalized_type AS type
FROM datacite.works dw
INNER JOIN type_map ON dw.type = type_map.original_type;