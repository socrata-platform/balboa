package com.blist.metrics;

import com.socrata.balboa.metrics.Metric;


public interface MetricQueue
{
    public static final long AGGREGATE_GRANULARITY = 120 * 1000;

    void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type);

    void create(String entityId, String name, Number value, long timestamp);

    void create(String entityId, String name, Number value);


    void logViewChildLoaded(String parentViewUid, String childViewUid, String displaytype);

    void logViewSearch(String viewUid, String query);

    void logUserSearch(int domainId, String query);

    void logDatasetSearch(String domainId, String query);

    void logUserCreated(String domainId);

    void logFilteredViewCreated(String domainId, String parentViewUid);

    void logFilteredViewDeleted(int domainId, String parentViewUid);

    void logDatasetCreated(int domainId, String viewUid, boolean isBlob, boolean isHref);

    void logDatasetDeleted(int domainId, String viewUid, boolean isBlob, boolean isHref);

    void logMapCreated(int domainId, String parentUid);

    void logMapDeleted(int domainId, String parentViewUid);

    void logChartCreated(int domainId, String parentViewUid);

    void logChartDeleted(int domainId, String parentViewUid);

    void logRowsCreated(int count, int domainId, String viewUid, String appTokenId);

    void logRowsDeleted(int count, int domainId, String viewUid, String appTokenUid);

    void logPublish(String url, String viewUid, int domainId);

    void logDatasetReferrer(String url, String viewUid);

    void logAction(Action type, String viewUid, int domainId, Integer value, String tokenUid);

    void logRowAccess(AccessChannel type, int domainId, String viewUid, Integer count, String appTokenUid);

    void logBytesInOrOut(String inOrOut, String viewUid, int domainId, Long bytes);

    void logGeocoding(int domainId, String viewUid, int count);

    void logDatasetDiskUsage(long timestamp, String viewUid, String authorUid, long bytes);

    void logDomainDiskUsage(long timestamp, int domainId, long bytes);

    void logUserDomainDiskUsage(long timestamp, String userUid, int domainId, long bytes);

    void logAppTokenCreated(int domainId);

    void logAppTokenRequest(String tokenUid, int domainId, String ip);

    void logAppTokenRequestForView(String viewUid, String token);

    void logApiQueryForView(String viewUid, String query);

    void logSocrataAppTokenUsed(String ip);

    enum Action {
        SHARE,
        COMMENT,
        RATING,
        VIEW,
        FAVORITE;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    enum Import {
        REPLACE,
        IMPORT,
        APPEND;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    enum AccessChannel {
        DOWNLOAD,
        WEBSITE,
        API,
        PRINT,
        WIDGET,
        RSS,
        EMAIL,
        UNKNOWN;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }
}
