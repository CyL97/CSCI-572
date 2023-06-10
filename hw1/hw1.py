import json
import time
import csv
from bs4 import BeautifulSoup
import requests
from random import randint
from html.parser import HTMLParser

USER_AGENT = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep:  # Prevents loading too many pages too soon
            time.sleep(randint(2, 5))
        temp_url = '+'.join(query.split())
        url = 'https://www.duckduckgo.com/html/?q=' + temp_url
        soup = BeautifulSoup(requests.get(
            url, headers=USER_AGENT).text, "html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all("a", attrs={"class": "result__a"})
        #print(type(raw_results))
        results = []
        if len(raw_results) < 10:
            threshold = len(raw_results)
        else:
            threshold = 10
        for result in raw_results:
            link = result.get('href')
            if link not in results:
                results.append(link)
            if len(results) >= threshold:
                break
        #print(len(results))
        return results

def read_queries(filename):
    with open(filename) as f:
        lines = f.read().splitlines()
    return lines

def find_matches(ddg, google):
    matches = []
    #i = 0
    for query in ddg:
        temp = []
        #print(len(ddg[query]))
        #i += 1
        for url in range(len(ddg[query])):
            if ddg[query][url] in google[query]:
                temp.append([google[query].index(ddg[query][url]) + 1, url + 1])
        #print(temp)
        matches.append(temp)
    #print(i)
    return matches

def spearman_cofficient(data):
    overlap_list = []
    overlap_percent_list = []
    spearman_cofficient_list = []
    sum_overlap = 0
    sum_overlap_percent = 0
    sum_spearman_cofficient = 0

    for matches in data:
        n = len(matches)
        overlap_list.append(n)
        sum_overlap += n

        percent = round(len(matches)/10 * 100.0, 1)
        overlap_percent_list.append(percent)
        sum_overlap_percent += percent

        d2s = []
        if n == 0:
            spearman_cofficient_list.append(0)
        else:
            for match in matches:
                d2 = (match[0] - match[1])**2
                d2s.append(d2)
            # If n = 1(which means only one paired match), we deal with it in a different way:
            # 1. if Rank in your result = Rank in Google result → rho = 1
            # 2. if Rank in your result ≠ Rank in Google result → rho = 0
            if n == 1:
                if match[0] == match[1]:
                    spearman_cofficient_list.append(1)
                    sum_spearman_cofficient += 1
                else:
                    spearman_cofficient_list.append(0)
            else:
                #print(n)
                spearman_cofficient = 1 - 6 * sum(d2s) / (n * (n**2 - 1))
                sum_spearman_cofficient += spearman_cofficient
                spearman_cofficient_list.append(spearman_cofficient)

    # Calculate averages
    avg_overlap = sum_overlap / 100
    avg_overlap_percent = sum_overlap_percent / 100
    avg_spearman_cofficient = sum_spearman_cofficient / 100
    return overlap_list, overlap_percent_list, spearman_cofficient_list, avg_overlap, avg_overlap_percent, avg_spearman_cofficient

if __name__ == '__main__':
    query_file = "./100QueriesSet4.txt"
    Google_file = "./Google_Result4.json"
    hw1_json = "./hw1.json"
    hw1_csv = "./hw1.csv"

    DuckDuckGo = SearchEngine()
    results = {}

    # Get DuckDuckGo's URLs
    '''queries = read_queries(query_file)
    for query in queries:
        result = query.rstrip()
        results[result] = DuckDuckGo.search(query)
    out_json = json.dumps(results, indent=2)
    with open(hw1_json, 'w') as f:
        f.write(out_json)'''

    # Get matches between Google and DuckDuckGo
    ddg_result = json.load(open(hw1_json))
    google_result = json.load(open(Google_file))
    overlap = find_matches(ddg_result, google_result)
    #print(overlap)

    # Calculate Spearman Cofficient
    overlap_list, overlap_percent_list, spearman_cofficient_list, avg_overlap, avg_overlap_percent, avg_spearman_cofficient = spearman_cofficient(overlap)
    #print(overlap_list)
    #print(overlap_percent_list)
    #print(spearman_cofficient_list)
    #print(avg_overlap)
    #print(avg_overlap_percent)
    #print(avg_spearman_cofficient)

    result_str = 'Queries, Number of Overlapping Results, Percent Overlap, Spearman Coefficient\n'
    for i in range(len(overlap_list)):
        temp_str = 'Query ' + str(i + 1) + ', ' + str(overlap_list[i]) + ', ' + str(overlap_percent_list[i]) + ', ' + str(spearman_cofficient_list[i]) + '\n'
        result_str += temp_str
    temp_str = 'Averages, ' + str(avg_overlap) + ', ' + str(avg_overlap_percent) + ', ' + str(avg_spearman_cofficient)
    result_str += temp_str
    print(result_str)
    with open(hw1_csv, 'w') as f:
        f.write(result_str)
    #write_csv(results, 'hw1.csv')