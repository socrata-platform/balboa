package com.blist.metrics.impl.queue;

import com.blist.metrics.MetricQueue;
import com.socrata.balboa.metrics.Metric;
import org.apache.commons.lang.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

public abstract class AbstractMetricQueue implements MetricQueue {
    public static final int MAX_URL_SIZE = 500;

    public void create(String entityId, String name, Number value, long timestamp) {
        create(entityId, name, value, timestamp, Metric.RecordType.AGGREGATE);
    }

    public void create(String entityId, String name, Number value) {
        create(entityId, name, value, new Date().getTime());
    }

    public void logViewChildLoaded(String parentViewUid, String childViewUid, String displaytype) {
        if (displaytype == null) {
            create("children-loaded-" + parentViewUid, "filter-"
                    + childViewUid, 1);
        } else if (displaytype.equals("map")) {
            create("children-loaded-" + parentViewUid, "map-"
                    + childViewUid, 1);
        } else if (displaytype.equals("chart")) {
            create("children-loaded-" + parentViewUid, "chart-"
                    + childViewUid, 1);
        }
    }

    public void logViewSearch(String view, String query) {
        create("searches-" + view, "search-" + query, 1);
    }

    public void logUserSearch(int domainId, String query) {
        create("searches-" + domainId, "users-search-" + query, 1);
    }

    public void logDatasetSearch(String domainId, String query) {
        create("searches-" + domainId, "datasets-search-" + query, 1);
    }

    public void logUserCreated(String domainId) {
        create(String.valueOf(domainId), "users-created", 1);
    }

    public void logAppTokenCreated(int domainId) {
        create(String.valueOf(domainId), "app-token-created", 1);
    }

    public void logAppTokenRequest(String tokenUid, int domainId, String ip) {
        create("ip-applications", "application-" + tokenUid + "-" + ip, 1);
        create(domainId + "-applications", "application-" + tokenUid, 1);
        create("applications", "application-" + tokenUid, 1);
        create("application-" + tokenUid, "requests", 1);
    }

    public void logAppTokenRequestForView(String viewUid, String token) {
        create("view-" + viewUid + "-apps", (token == null ? "anon" : token), 1);
    }

    public void logApiQueryForView(String viewUid, String query) {
        create("view-" + viewUid + "-query", query == null || query.equals("") ? "select *" : query, 1);
        create(viewUid, "queries-served", 1);
    }

    public void logSocrataAppTokenUsed(String ip) {
        //create("ip-socrata-token-used", ip, 1);
    }

    public void logMapCreated(int domainId, String parentUid) {
        create(String.valueOf(domainId), "maps-created", 1);
        create(parentUid, "maps-created", 1);
    }

    public void logMapDeleted(int domainId, String parentViewUid) {
        create(String.valueOf(domainId), "maps-deleted", 1);
        create(parentViewUid, "maps-deleted", 1);
    }

    public void logChartCreated(int domainId, String parentViewUid) {
        create(String.valueOf(domainId), "charts-created", 1);
        create(parentViewUid, "charts-created", 1);
    }

    public void logChartDeleted(int domainId, String parentViewUid) {
        create(String.valueOf(domainId), "charts-deleted", 1);
        create(parentViewUid, "charts-deleted", 1);
    }

    public void logFilteredViewCreated(String domainId, String parentViewUid) {
        create(String.valueOf(domainId), "filters-created", 1);
        create(parentViewUid, "filters-created", 1);
    }

    public void logFilteredViewDeleted(int domainId, String parentViewUid) {
        create(String.valueOf(domainId), "filters-deleted", 1);
        create(parentViewUid, "filters-deleted", 1);
    }

    public void logDatasetCreated(int domainId, String viewUid, boolean isBlob, boolean isHref) {
        String idStr = String.valueOf(domainId);

        if (isBlob) {
            create(idStr, "datasets-created-blobby", 1);
        }

        if (isHref) {
            create(idStr, "datasets-created-href", 1);
        }

        create(idStr, "datasets-created", 1);
    }

    public void logDatasetDeleted(int domainId, String viewUid, boolean isBlob, boolean isHref) {
        String idStr = String.valueOf(domainId);
        if (isBlob) {
            create(idStr, "datasets-deleted-blobby", 1);
        }

        if (isHref) {
            create(idStr, "datasets-deleted-href", 1);
        }

        create(idStr, "datasets-deleted", 1);
    }

    public void logRowsCreated(int count, int domainId, String viewUid, String token) {
        create(String.valueOf(domainId), "rows-created", count);
        create(viewUid, "rows-created", count);
    }

    public void logRowsDeleted(int count, int domainId, String viewUid, String token) {
        create(String.valueOf(domainId), "rows-deleted", count);
        create(viewUid, "rows-deleted", count);

        logAppTokenOnView(viewUid, token);
    }

    public void logDatasetReferrer(String referrer, String viewUid) {
        if (referrer.length() > MAX_URL_SIZE) {
            referrer = referrer.substring(0, MAX_URL_SIZE);
        }

        try {
            URL url = new URL(referrer);


            String host = url.getProtocol() + "-" + url.getHost();
            String path = url.getPath();
            if (!StringUtils.isBlank(url.getQuery())) {
                path += "?" + url.getQuery();
            }

            create("referrer-hosts-" + viewUid, "referrer-" + host, 1);
            create("referrer-paths-" + viewUid + "-" + host, "path-"
                    + path, 1);
        } catch (MalformedURLException e) {
            // Oh well, shouldn't log this anyway.
        }
    }

    public void logPublish(String referrer, String viewUid, int domainId) {
        try {
            URL url = new URL(referrer);


            String host = url.getProtocol() + "-" + url.getHost();
            String path = url.getPath();
            if (!StringUtils.isBlank(url.getQuery())) {
                path += "?" + url.getQuery();
            }

            create("publishes-uids-" + domainId, "uid-" + viewUid, 1);

            create("publishes-hosts-" + viewUid, "referrer-" + host, 1);
            create("publishes-paths-" + viewUid + "-" + host, "path-"
                    + path, 1);

            create("publishes-hosts-" + domainId, "referrer-" + host,
                    1);
            create("publishes-paths-" + domainId + "-" + host, "path-"
                    + path, 1);
        } catch (MalformedURLException e) {
            // Well, that doesn't look like a URL, so I probably shouldn't log
            // it
        }
    }

    public void logAction(Action type, String viewUid, int domainId,
                          Integer value, String tokenUid) {
        String idStr = String.valueOf(domainId);

        if (value == null) {
            value = 1;
        }

        if (type == Action.RATING) {
            create(viewUid, "ratings-total", value);
            create(viewUid, "ratings-count", 1);

            create(idStr, "ratings-total", value);
            create(idStr, "ratings-count", 1);
        } else if (type == Action.VIEW) {
            create(viewUid, "view-loaded", 1);
            create(idStr, "view-loaded", 1);

            create("views-loaded-" + idStr, "view-"
                    + viewUid, 1);
        } else {
            create(viewUid, type.toString() + "s", value);
            create(idStr, type.toString() + "s", value);
        }

        logAppTokenOnView(viewUid, tokenUid);
    }

    public void logBytesInOrOut(String inOrOut, String viewUid, int domainId,
                                Long bytes) {
        if (viewUid != null) {
            create(viewUid, "bytes-" + inOrOut, bytes);
        }

        create(String.valueOf(domainId), "bytes-" + inOrOut, bytes);

    }

    public void logRowAccess(AccessChannel type, int domainId,
                             String viewUid, Integer count, String token) {
        String idStr = String.valueOf(domainId);

        create(viewUid, "rows-accessed-" + type, 1);
        create(idStr, "rows-accessed-" + type, 1);
        if (AccessChannel.DOWNLOAD == type) {
            create("views-downloaded-" + idStr, "view-" + viewUid, 1);
        }

        create(viewUid, "rows-loaded-" + type, count);
        create(idStr, "rows-loaded-" + type, count);

        logAppTokenOnView(viewUid, token);
    }

    public void logGeocoding(int domainId, String viewUid, int count) {
        create(viewUid, "geocoding-requests", count);
        create(String.valueOf(domainId), "geocoding-requests", count);
    }

    public void logDatasetDiskUsage(long timestamp, String viewUid, String authorUid, long bytes) {
        create(viewUid, "disk-usage", bytes, timestamp, Metric.RecordType.ABSOLUTE);
        create("user-" + authorUid + "-views-disk-usage", "view-" + viewUid, bytes, timestamp, Metric.RecordType.ABSOLUTE);
    }

    public void logDomainDiskUsage(long timestamp, int domainId, long bytes) {
        create(String.valueOf(domainId), "disk-usage", bytes, timestamp, Metric.RecordType.ABSOLUTE);
    }

    public void logUserDomainDiskUsage(long timestamp, String userUid, int domainId, long bytes) {
        create(domainId + "-users-disk-usage", "user-" + userUid, bytes, timestamp, Metric.RecordType.ABSOLUTE);
    }

    private void logAppTokenOnView(String realViewUid, String token) {
        if (token != null) {
            create(realViewUid + "-apps", token, 1);
            create("app-" + token, realViewUid, 1);
        }
    }
}
