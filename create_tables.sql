DROP TABLE IF EXISTS login_failed;
DROP TABLE IF EXISTS login_succeeded;
DROP TABLE IF EXISTS playback_started;
DROP TABLE IF EXISTS user_created;
DROP TABLE IF EXISTS visitor_landed;

CREATE TABLE visitor_landed (
    envelope_eventId UUID PRIMARY KEY,
    envelope_eventTimestamp TIMESTAMPTZ NOT NULL,
    envelope_eventName VARCHAR(255) NOT NULL,
    envelope_eventVersion VARCHAR(50),
    envelope_source VARCHAR(255),
    envelope_domain VARCHAR(255),
    payload_anonymousId TEXT NOT NULL,
    payload_landingPageUrl TEXT,
    payload_attribution_source VARCHAR(255),
    payload_attribution_medium VARCHAR(255),
    payload_attribution_campaign TEXT,
    payload_device_type VARCHAR(100),
    payload_device_browser VARCHAR(100),
    payload_device_os VARCHAR(100),
    payload_geolocation_country VARCHAR(100),
    payload_geolocation_region VARCHAR(100),
    payload_geolocation_city VARCHAR(255),
    payload_browserLanguage VARCHAR(50)
);

CREATE TABLE user_created (
	envelope_eventId UUID PRIMARY KEY,
	envelope_eventTimestamp TIMESTAMPTZ NOT NULL,
	envelope_eventName VARCHAR(255) NOT NULL,
	envelope_eventVersion VARCHAR(50),
	envelope_source VARCHAR(255),
	envelope_domain VARCHAR(255),
	payload_userId TEXT NOT NULL,
	payload_anonymousId TEXT NOT NULL,
	payload_emailHash VARCHAR(255),
	payload_initialPlanId VARCHAR(100),
	payload_acquisitionChannel VARCHAR(100)
);

CREATE TABLE playback_started (
	envelope_eventId UUID PRIMARY KEY,
	envelope_eventTimestamp TIMESTAMPTZ NOT NULL,
	envelope_eventName VARCHAR(255) NOT NULL,
	envelope_eventVersion VARCHAR(50),
	envelope_source VARCHAR(255),
	envelope_domain VARCHAR(255),
	payload_userId TEXT NOT NULL,
	payload_profileId TEXT NOT NULL,
	payload_playback_sessionid TEXT NOT NULL,
	payload_videoId INTEGER NOT NULL,
	payload_videoType VARCHAR(255),
	payload_device_type VARCHAR(100),
	payload_device_manufacturer VARCHAR(100),
	payload_device_os VARCHAR(100),
	payload_trigger VARCHAR(100),
	payload_playbackStartTime INTEGER
);

CREATE TABLE login_succeeded (
	envelope_eventId UUID PRIMARY KEY,
	envelope_eventTimestamp TIMESTAMPTZ NOT NULL,
	envelope_eventName VARCHAR(255) NOT NULL,
	envelope_eventVersion VARCHAR(50),
	envelope_source VARCHAR(255),
	envelope_domain VARCHAR(255),
	payload_userId TEXT NOT NULL,
	payload_loginType VARCHAR(100),
	payload_isNewDevice BOOLEAN
);

CREATE TABLE login_failed (
	envelope_eventId UUID PRIMARY KEY,
	envelope_eventTimestamp TIMESTAMPTZ NOT NULL,
	envelope_eventName VARCHAR(255) NOT NULL,
	envelope_eventVersion VARCHAR(50),
	envelope_source VARCHAR(255),
	envelope_domain VARCHAR(255),
	payload_emailAttempted VARCHAR(100) NOT NULL,
	payload_failureReason VARCHAR(100) NOT NULL,
	payload_consecutiveFailureCount INTEGER
);


