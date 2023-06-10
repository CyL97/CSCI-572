import java.util.*;

class Fetch{
    String url;
    int statusCode;
    public Fetch(String url, int statusCode) {
        this.url = url;
        this.statusCode = statusCode;
    }
}

class Visit {
    String url;
    int size;
    int numOutlinks;
    String contentType;
    public Visit(String url, int size, int numOutlinks, String contentType) {
        this.url = url;
        this.size = size;
        this.numOutlinks = numOutlinks;
        this.contentType = contentType;
    }
}

class Discover {
    String url;
    String resIndicator;
    public Discover(String url, String resIndicator) {
        this.url = url;
        this.resIndicator = resIndicator;
    }
}

public class UrlProcessor {
    ArrayList<Fetch> fetch;
    ArrayList<Visit> visit;
    ArrayList<Discover> discover;

    public UrlProcessor() {
        fetch = new ArrayList<Fetch>();
        visit = new ArrayList<Visit>();
        discover = new ArrayList<Discover>();
    }

    public void addFetch(String url, int statusCode) {
        this.fetch.add(new Fetch(url, statusCode));
    }

    public void addVisit(String url, int size, int numOutlinks, String contentType) {
        this.visit.add(new Visit(url, size, numOutlinks, contentType));
    }

    public void addDiscover(String url, String resIndicator) {
        this.discover.add(new Discover(url, resIndicator));
    }
}
