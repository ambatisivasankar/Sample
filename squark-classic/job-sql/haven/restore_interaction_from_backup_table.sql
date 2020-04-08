-- If somehow the interaction table exists still, get rid of it
DROP TABLE IF EXISTS HAVEN.INTERACTION;

-- Add interaction table back in to haven_uw schema
SELECT COPY_TABLE('SQUARK_STAGING.HAVEN_INTERACTION_BACKUP', 'HAVEN.INTERACTION');

-- Grants
GRANT ALL  PRIVILEGES  EXTEND  ON TABLE haven.interaction  TO  haven_admin;
GRANT SELECT  ON TABLE haven.interaction  TO  haven_view,haven;
