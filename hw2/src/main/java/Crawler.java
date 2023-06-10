import java.util.Set;
import java.util.regex.Pattern;
import edu.uci.ics.crawler4j.crawler.*;
import edu.uci.ics.crawler4j.parser.*;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler extends WebCrawler {
    UrlProcessor urlProcessor;
    String siteDomain = "latimes.com";
    private final static Pattern FILTERS = Pattern.compile(".*(\\.(html?|php|pdf|docx?|jpe?g|ico|png|bmp|svg|gif|webp|tiff))$");

    public Crawler() {
        urlProcessor = new UrlProcessor();
    }

    public static String normalization(String url) {
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        // Replace comma to be '_'
        return url.toLowerCase().replace(",","_").replaceFirst("^(https?://)?(www.)?", "");
    }

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = normalization(url.getURL());
        if (href.startsWith(siteDomain)) {
            urlProcessor.addDiscover(url.getURL(), "OK");
        }
        else {
            urlProcessor.addDiscover(url.getURL(), "N_OK");
        }
        return !FILTERS.matcher(href).matches() && href.startsWith(siteDomain);
    }

    @Override
    public void handlePageStatusCode(WebURL url, int statusCode, String statusDescription) {
        urlProcessor.addFetch(url.getURL(), statusCode);
    }

    @Override
    public void visit(Page page) {
        String url = page.getWebURL().getURL();
        String contentType = page.getContentType().toLowerCase().split(";")[0];
        //System.out.println(contentType);

        int size = page.getContentData().length;
        int numOutlinks;

        if (contentType.equals("text/html"))
        {
            if (page.getParseData() instanceof HtmlParseData)
            {
                HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
                Set<WebURL> outgoingUrls = htmlParseData.getOutgoingUrls();
                numOutlinks = outgoingUrls.size();
                urlProcessor.addVisit(url, size, numOutlinks, contentType);
            }
        }
        else if (contentType.startsWith("image") || contentType.equals("application/pdf") || contentType.equals("application/document") || contentType.equals("application/msword")) {
            numOutlinks = 0;
            urlProcessor.addVisit(url, size, numOutlinks, contentType);
        }
        // Ignore other content type
    }

    @Override
    public Object getMyLocalData() {
        return urlProcessor;
    }
}
