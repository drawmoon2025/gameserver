-- Apply to every physical shard.

CREATE TABLE IF NOT EXISTS account_bind (
  provider  VARCHAR(32)  NOT NULL,
  -- Binary: a partner's IDs may differ only by case, and must not merge accounts.
  subject   VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  uid       BIGINT       NOT NULL,
  created_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (provider, subject),
  KEY idx_uid (uid)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS account_password (
  username  VARCHAR(64)    NOT NULL PRIMARY KEY,
  hash      VARBINARY(128) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS wallet_account (
  uid       BIGINT NOT NULL PRIMARY KEY,
  balance   BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT chk_balance CHECK (balance >= 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS wallet_ledger (
  id            BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  uid           BIGINT      NOT NULL,
  request_id    VARCHAR(64) NOT NULL,
  delta         BIGINT      NOT NULL,
  reason        VARCHAR(32) NOT NULL,
  fingerprint   VARCHAR(64) NOT NULL,
  balance_after BIGINT      NOT NULL,
  result        BLOB        NULL,
  created_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_request (uid, request_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_outbox (
  id            BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  topic         VARCHAR(64)  NOT NULL,
  partition_key VARCHAR(64)  NOT NULL,
  payload       MEDIUMBLOB   NOT NULL
) ENGINE=InnoDB;

-- SLG durable timers. Partition by shard; one scheduler owns a shard.
CREATE TABLE IF NOT EXISTS timer_shard (
  shard INT         NOT NULL PRIMARY KEY,
  token BIGINT      NOT NULL,
  owner VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS timer_job (
  id       BIGINT      NOT NULL,
  shard    INT         NOT NULL,
  kind     VARCHAR(32) NOT NULL,
  payload  BLOB        NOT NULL,
  fire_at  DATETIME(3) NOT NULL,
  attempt  INT         NOT NULL DEFAULT 0,
  PRIMARY KEY (shard, id),
  KEY idx_due (shard, fire_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS slg_march (
  id        BIGINT      NOT NULL PRIMARY KEY,
  kingdom   VARCHAR(32) NOT NULL,
  uid       BIGINT      NOT NULL,
  request_id VARCHAR(64) NOT NULL,
  from_x    INT         NOT NULL,
  from_y    INT         NOT NULL,
  to_x      INT         NOT NULL,
  to_y      INT         NOT NULL,
  troops    BIGINT      NOT NULL,
  arrive_at DATETIME(3) NOT NULL,
  state     TINYINT     NOT NULL DEFAULT 0, -- 0 marching, 1 arrived
  KEY idx_uid (uid, state),
  UNIQUE KEY uk_request (uid, request_id)
) ENGINE=InnoDB;
