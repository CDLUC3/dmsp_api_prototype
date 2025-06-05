CREATE OR REPLACE TABLE datacite_types AS

WITH type_map AS (
  -- Mapping table to normalise DataCite types
  SELECT * FROM (VALUES
    ('JournalArticle', 'article'),
    ('ConferencePaper', 'article'),
    ('Audiovisual', 'audio-visual'),
    ('Film', 'audio-visual'),
    ('Book', 'book'),
    ('BookChapter', 'book-chapter'),
    ('Collection', 'collection'),
    ('DataPaper', 'data-paper'),
    ('Dataset', 'dataset'),
    ('Dissertation', 'dissertation'),
    ('Event', 'event'),
    ('Image', 'image'),
    ('InteractiveResource', 'interactive-resource'),
    ('Model', 'model'),
    ('OutputManagementPlan', 'output-management-plan'),
    ('PeerReview', 'peer-review'),
    ('PhysicalObject', 'physical-object'),
    ('Instrument', 'physical-object'),
    ('Preprint', 'preprint'),
    ('Report', 'report'),
    ('Service', 'service'),
    ('Software', 'software'),
    ('ComputationalNotebook', 'software'),
    ('Sound', 'sound'),
    ('Standard', 'standard'),
    ('Text', 'text'),
    ('Workflow', 'workflow'),
    ('Other', 'other')
  ) AS t(original_type, normalized_type)
)

SELECT
  doi,
  type_map.normalized_type AS type
FROM datacite_works dw
INNER JOIN type_map ON dw.type = type_map.original_type