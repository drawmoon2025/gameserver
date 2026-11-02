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

-- The server list. SERVERS seeds it at startup.
CREATE TABLE IF NOT EXISTS kingdom (
  id         VARCHAR(32)  NOT NULL PRIMARY KEY,
  name       VARCHAR(64)  NOT NULL,
  ws_url     VARCHAR(255) NOT NULL,
  capacity   INT          NOT NULL,
  population INT          NOT NULL DEFAULT 0
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS player_kingdom (
  uid     BIGINT      NOT NULL PRIMARY KEY, -- One kingdom per player.
  kingdom VARCHAR(32) NOT NULL,
  x       INT         NOT NULL,
  y       INT         NOT NULL
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
  home      TINYINT(1)  NOT NULL DEFAULT 0, -- A recalled army going back to its city.
  KEY idx_uid (uid, state),
  UNIQUE KEY uk_request (uid, request_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS slg_tile (
  kingdom VARCHAR(32) NOT NULL,
  x       INT         NOT NULL,
  y       INT         NOT NULL,
  uid     BIGINT      NOT NULL,
  troops  BIGINT      NOT NULL,
  PRIMARY KEY (kingdom, x, y),
  KEY idx_uid (uid, kingdom)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS city_state (
  uid     BIGINT      NOT NULL PRIMARY KEY,
  gold    BIGINT      NOT NULL,
  gold_at DATETIME(3) NOT NULL,
  troops  BIGINT      NOT NULL DEFAULT 0,
  CONSTRAINT chk_troops CHECK (troops >= 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS city_building (
  uid   BIGINT      NOT NULL,
  kind  VARCHAR(16) NOT NULL,
  level INT         NOT NULL,
  PRIMARY KEY (uid, kind)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS city_upgrade (
  id         BIGINT      NOT NULL PRIMARY KEY,
  uid        BIGINT      NOT NULL,
  kind       VARCHAR(16) NOT NULL,
  to_level   INT         NOT NULL,
  request_id VARCHAR(64) NOT NULL,
  finish_at  DATETIME(3) NOT NULL,
  state      TINYINT     NOT NULL DEFAULT 0, -- 0 in progress, 1 done
  UNIQUE KEY uk_request (uid, request_id),
  KEY idx_uid (uid, state)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS city_training (
  id         BIGINT      NOT NULL PRIMARY KEY,
  uid        BIGINT      NOT NULL,
  count      BIGINT      NOT NULL,
  request_id VARCHAR(64) NOT NULL,
  finish_at  DATETIME(3) NOT NULL,
  state      TINYINT     NOT NULL DEFAULT 0, -- 0 in progress, 1 done
  UNIQUE KEY uk_request (uid, request_id),
  KEY idx_uid (uid, state)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS city_grant (
  uid       BIGINT      NOT NULL,
  grant_key VARCHAR(64) NOT NULL,
  PRIMARY KEY (uid, grant_key)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS alliance (
  name   VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
  leader BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS alliance_member (
  uid      BIGINT NOT NULL PRIMARY KEY, -- One alliance per player.
  alliance VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  seq      BIGINT NOT NULL AUTO_INCREMENT,
  UNIQUE KEY uk_seq (seq),
  KEY idx_alliance (alliance, seq)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS mail_box (
  id          BIGINT       NOT NULL PRIMARY KEY,
  uid         BIGINT       NOT NULL,
  dedupe_key  VARCHAR(64)  NOT NULL,
  kind        VARCHAR(16)  NOT NULL,
  title       VARCHAR(128) NOT NULL,
  body        TEXT         NOT NULL,
  reward_gold BIGINT       NOT NULL DEFAULT 0,
  claimed     TINYINT(1)   NOT NULL DEFAULT 0,
  sent_at     DATETIME(3)  NOT NULL,
  UNIQUE KEY uk_dedupe (uid, dedupe_key),
  KEY idx_list (uid, id)
) ENGINE=InnoDB;
