import edu.uci.ics.crawler4j.crawler.*;
import edu.uci.ics.crawler4j.fetcher.*;
import edu.uci.ics.crawler4j.robotstxt.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;

public class App {
    static int numberOfCrawlers = 64;
    static UrlProcessor urlProcessor;


    public static void main(String[] args) throws Exception {
        urlProcessor = new UrlProcessor();

        String crawlStorageFolder = "./crawl";

        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorageFolder);
        config.setMaxPagesToFetch(20000);
        config.setMaxDepthOfCrawling(16);
        config.setPolitenessDelay(50);
        config.setIncludeBinaryContentInCrawling(true);

        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

        controller.addSeed("https://www.latimes.com");
        controller.start(Crawler.class, numberOfCrawlers);

        for (Object i : controller.getCrawlersLocalData()) {
            UrlProcessor data = (UrlProcessor) i;
            urlProcessor.fetch.addAll(data.fetch);
            urlProcessor.visit.addAll(data.visit);
            urlProcessor.discover.addAll(data.discover);
        }

        fetchCSV();
        visitCSV();
        urlCSV();
        report();
    }


    private static void fetchCSV() throws Exception {

        File newFile = new File("fetch_latimes.csv");
        newFile.delete();
        newFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(newFile, true));
        bw.write("URL,Status Code\n");

        for (Fetch fetch : urlProcessor.fetch) {
            bw.write(fetch.url + "," + fetch.statusCode + "\n");
        }
        bw.close();
    }

    private static void visitCSV() throws Exception {
        File newFile = new File("visit_latimes.csv");
        newFile.delete();
        newFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(newFile, true));
        bw.write("URL,Size (Bytes),# of Outlinks,Content Type\n");

        for (Visit visit : urlProcessor.visit) {
            bw.write(visit.url + "," + visit.size + "," + visit.numOutlinks + "," + visit.contentType + "\n");
        }
        bw.close();
    }

    private static void urlCSV() throws Exception {
        File newFile = new File("urls_latimes.csv");
        newFile.delete();
        newFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(newFile, true));
        bw.write("URL,Residence Indicator\n");

        for (Discover discover : urlProcessor.discover) {
            bw.write(discover.url + "," + discover.resIndicator + "\n");
        }
        bw.close();
    }


    private static void report() throws Exception {
        File newFile = new File("CrawlReport_latimes.txt");
        newFile.delete();
        newFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(newFile, true));
        bw.write("Name: Chaoyu Li\n");
        bw.write("USC ID: 6641732094\n");
        bw.write("News site crawled: latimes.com\n");
        bw.write("Number of threads: " + numberOfCrawlers + "\n\n");

        // Fetch statistics
        int total = urlProcessor.fetch.size();
        HashMap<Integer, Integer> statusCodes = new HashMap<Integer, Integer>();
        for (Fetch fetch : urlProcessor.fetch) {
            if (statusCodes.containsKey(fetch.statusCode)) {
                statusCodes.put(fetch.statusCode, statusCodes.get(fetch.statusCode) + 1);
            } else {
                statusCodes.put(fetch.statusCode, 1);
            }
        }
        int succeeded = statusCodes.get(200);

        bw.write("Fetch Statistics\n");
        bw.write("================\n");
        bw.write("# fetches total: " + total + "\n");
        bw.write("# fetches succeeded: " + succeeded + "\n");
        bw.write("# fetches failed or aborted: " + (total - succeeded) + "\n\n");

        // Outgoing Urls
        int numUrls = urlProcessor.discover.size();
        int numUrlsWithin = 0;
        HashSet<String> uniqueUrls = new HashSet<String>();
        for (Discover discover : urlProcessor.discover) {
            if (!uniqueUrls.contains(discover.url)) {
                if (discover.resIndicator.equals("OK")) {
                    numUrlsWithin++;
                }
                uniqueUrls.add(discover.url);
            }
        }
        int numUniqueUrls = uniqueUrls.size();

        bw.write("Outgoing URLs:\n");
        bw.write("==============\n");
        bw.write("Total URLs extracted: " + numUrls + "\n");
        bw.write("# unique URLs extracted: " + numUniqueUrls + "\n");
        bw.write("# unique URLs within News Site: " + numUrlsWithin + "\n");
        bw.write("# unique URLs outside News Site: " + (numUniqueUrls - numUrlsWithin) + "\n\n");

        //Status Codes
        bw.write("Status Codes:\n");
        bw.write("=============\n");
        if (statusCodes.get(200) != null) {
            bw.write("200 OK: " + statusCodes.get(200) + "\n");
        }
        if (statusCodes.get(301) != null) {
            bw.write("301 Moved Permanently: " + statusCodes.get(301) + "\n");
        }
        if (statusCodes.get(302) != null) {
            bw.write("302 Moved Temporarily: " + statusCodes.get(302) + "\n");
        }
        if (statusCodes.get(303) != null) {
            bw.write("303: See Other Resource: " + statusCodes.get(303) + "\n");
        }
        if (statusCodes.get(304) != null) {
            bw.write("304: Not Modified: " + statusCodes.get(304) + "\n");
        }
        if (statusCodes.get(305) != null) {
            bw.write("305: Use Proxy: " + statusCodes.get(305) + "\n");
        }
        if (statusCodes.get(306) != null) {
            bw.write("306: Switch Proxy: " + statusCodes.get(306) + "\n");
        }
        if (statusCodes.get(307) != null) {
            bw.write("307: Temporary Redirect: " + statusCodes.get(307) + "\n");
        }
        if (statusCodes.get(308) != null) {
            bw.write("308: Permanently Redirect: " + statusCodes.get(308) + "\n");
        }
        if (statusCodes.get(400) != null) {
            bw.write("400 Bad Request Response: " + statusCodes.get(400) + "\n");
        }
        if (statusCodes.get(401) != null) {
            bw.write("401 Unauthorized Request: " + statusCodes.get(401) + "\n");
        }
        if (statusCodes.get(403) != null) {
            bw.write("403 Access to Resource Forbidden: " + statusCodes.get(403) + "\n");
        }
        if (statusCodes.get(404) != null) {
            bw.write("404 Resource Not Found: " + statusCodes.get(404) + "\n");
        }
        if (statusCodes.get(405) != null) {
            bw.write("405 Method Not Allowed: " + statusCodes.get(405) + "\n");
        }
        if (statusCodes.get(406) != null) {
            bw.write("406 Not Acceptable: " + statusCodes.get(406) + "\n");
        }
        if (statusCodes.get(408) != null) {
            bw.write("408 Request Timeout: " + statusCodes.get(408) + "\n");
        }
        if (statusCodes.get(409) != null) {
            bw.write("409 Conflict: " + statusCodes.get(409) + "\n");
        }
        if (statusCodes.get(410) != null) {
            bw.write("410 Gone: " + statusCodes.get(410) + "\n");
        }
        if (statusCodes.get(500) != null) {
            bw.write("500 Internal Server Error: " + statusCodes.get(500) + "\n");
        }
        if (statusCodes.get(501) != null) {
            bw.write("501 Method Not Supported: " + statusCodes.get(501) + "\n");
        }
        if (statusCodes.get(502) != null) {
            bw.write("502 Gateway Error: " + statusCodes.get(502) + "\n");
        }
        if (statusCodes.get(503) != null) {
            bw.write("503 Service Unavailable: " + statusCodes.get(503) + "\n");
        }
        if (statusCodes.get(504) != null) {
            bw.write("504 Gateway Timeout: " + statusCodes.get(504) + "\n");
        }
        bw.write("\n");

        //File Sizes
        int oKB = 0, tKB = 0, hKB = 0, oMB = 0, mMB = 0;
        HashMap<String, Integer> contentTypes = new HashMap<String, Integer>();
        for (Visit visit : urlProcessor.visit) {
            int KB = 1024;
            if (visit.size < KB) {
                oKB++;
            } else if (visit.size < 10 * KB) {
                tKB++;
            } else if (visit.size < 100 * KB) {
                hKB++;
            } else if (visit.size < KB * KB) {
                oMB++;
            } else {
                mMB++;
            }

            if (contentTypes.containsKey(visit.contentType)) {
                contentTypes.put(visit.contentType, contentTypes.get(visit.contentType) + 1);
            } else {
                contentTypes.put(visit.contentType, 1);
            }
        }

        bw.write("File Sizes:\n");
        bw.write("===========\n");
        bw.write("< 1KB: " + oKB + "\n");
        bw.write("1KB ~ <10KB: " + tKB + "\n");
        bw.write("10KB ~ <100KB: " + hKB + "\n");
        bw.write("100KB ~ <1MB: " + oMB + "\n");
        bw.write(">= 1MB: " + mMB + "\n\n");

        // Content type
        bw.write("Content Types:\n");
        bw.write("==============\n");

        for (String type : contentTypes.keySet()) {
            bw.write(type + ": " + contentTypes.get(type) + "\n");
        }
        bw.close();

        for (int key : statusCodes.keySet()) {
            System.out.println(key + " " + statusCodes.get(key));
        }
    }
}