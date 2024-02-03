import logging
import re
from urllib.parse import parse_qs, urlparse, urljoin, parse_qsl, urlunparse
from lxml import html
from pathlib import Path
from bs4 import BeautifulSoup
logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.token_dict = {}
        self.blacklist = set()
        self.whitelist = set()
        self.similarity_threshold = 0.90
        self.n_length = 5
        self.stop_words = []
        self.page_most_links = {"link": "", "count": 0}
        self.subdomains = {}
        self.is_trap = False
        self.downloaded = set()
        self.removed = set()
        self.check_already = set()
        self.create_stop_words()
    

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            self.whitelist.add(url)
            url_data = self.corpus.fetch_url(url)
            # number of links that were able to fetched from the url
            valid_links_counter = 0
            self.downloaded.add(url)
            # Write links downloaded.txt
            for next_link in set(self.extract_next_links(url_data)):
                validity = self.is_valid(next_link)
                if validity:
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        valid_links_counter += 1
                else:
                    self.removed.add(next_link)
            # update link with the most valid links out
            if valid_links_counter > self.page_most_links["count"]:
                self.page_most_links = {
                    "link": url,
                    "count": valid_links_counter
                }


    def create_stop_words(self):
        """
        Reads stopwords.txt and adds all of its words in a list
        """
        with open("stopwords.txt", "r", encoding='utf-8') as stop_words_file:
            self.stop_words = stop_words_file.readlines()
        self.stop_words = [ x.strip().lower() for x in self.stop_words]
    

    def run_analytics(self):
        """
        Run the analytics after seperating all the crawler traps out
        """
        longest_page = {"link": "", "count": 0}
        vocabulary = {}
        counter = 0
        total_link = len(self.frontier.urls_set)
        with open("analytics.txt", "a", encoding="utf-8") as file:
            # Analytics 2: getting most valid out links
            file.write(f"Link with the most valid out links:\n{self.page_most_links}\n")
        with open("analytics.txt", "a", encoding="utf-8") as file:
            # Analytics 3: List of downloaded and list of identified traps
            file.write("\n\nList of downloaded urls:\n")
            for i in self.downloaded:
                file.write(f"URL: {i}\n")
        with open("analytics.txt", "a", encoding="utf-8") as file:
            file.write("\n\nList of identified trap urls:\n")
            for i in self.removed:
                file.write(f"URL: {i}\n")
        for link in self.frontier.urls_set:

            # -----------Analytics #1----------
            # counting urls each subdomains fetched
            subdomain = urlparse(link).netloc
            self.subdomains[subdomain] = self.subdomains.get(subdomain, 0) + 1

            # ----------Analytics #4--------
            url_data = self.corpus.fetch_url(link)
            if url_data["content_type"] is None:
                continue
            else:
                url_text_file = self.extract_words_generator(url_data).lower()
                if len(url_text_file) == 0:
                    continue
                url_text = []
                token = ""
                for letter in url_text_file:
                    if letter.isalnum() and letter.isascii():
                        token += letter.lower()
                    else:
                        if len(token) != 0:
                            # add token to list and reset word
                            url_text.append(token)
                            token = ""
                url_text_length = len(url_text)
                
                # finding largest page
                if url_text_length > longest_page["count"]:
                    longest_page = {
                        "link": link,
                        "count": url_text_length
                    }
                
                # accumulating counts for each word from all webpages and adding it to vocabulary
                url_text_set = set(url_text)
                for word in self.stop_words:
                    url_text_set.discard(word.lower())
                for word in url_text:
                    if word in url_text_set:
                        if word not in vocabulary:
                            vocabulary[word] = 0
                        vocabulary[word] += 1
            print(f'Generating report: {int((counter/total_link)*100)}%     \t--{counter}/{total_link}', end='\r')
            counter += 1

        with open("analytics.txt", "a", encoding="utf-8") as file:
            # Analytics 1: writing to file the subdomains and number of links
            file.write("\n\nSubdomains: Links proccessed\n")
            for subdomain, count in self.subdomains.items():
                file.write (f"{subdomain}: {count}\n")

        with open("analytics.txt", "a", encoding="utf-8") as file:
            # Analytics 4: writing to analytics page the longest page url and its count
            file.write(f"\n\nLongest Page: \n{longest_page}\n")            
        
        with open("analytics.txt", "a", encoding="utf-8") as file:
            # Analytics 5: writing to analytics the 50 most common words in all webpages and its count
            sorted_vocab = dict(sorted(vocabulary.items(), key=lambda x: x[1], reverse=True))
            file.write("\n\n50 most common words:\n")
            counter = 1
            for word, count in sorted_vocab.items():
                if counter == 51:
                    break
                file.write(f"{counter}. {word}: {count}\n")
                counter +=1

    

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. 
        This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        url = url_data.get('url')
        if url_data["is_redirected"]:
            url = url_data.get("final_url")
        content = url_data.get('content')

        if url is not None and content is not None and content != "" and len(content) != 0:
            # parses html content
            try:
                tree = html.fromstring(content)
                # gets all relative and absolute links
                all_links = tree.xpath("//a/@href")
                # turns every relative link into absolute
                absolute_urls =[urljoin(url, link) for link in all_links]
                return absolute_urls
            except Exception as e:
                print(e)
                return []
        else:
            return []


    def extract_words_generator(self, url_data):
        """
        Function that gets the file content. It grabs the anything in the p, h1, h2, h3, h4, h5, h6, span and div tabs. (Tabs that generally include words)
        This function returns the list of words in that file.
        """
        try:
            return BeautifulSoup(url_data["content"], "lxml").get_text()
        except Exception as e:
            print(e)
            return ""
    

    def check_similarity(self, parsed, url):
        """
        Tokenize the words and htei frequency. Checks if they are in the dictionary,
        if not then add.
        
        If it is found, do a similarity test and update the
        black and whitelist.
        """
        new_token = {
                "content": {},
                "blacklist": 0,
                "count_checks": 1,
                "link": url
            }
        word_list = []
        phrase = ""

        # if there is nothing on the page, return false
        url_data = self.corpus.fetch_url(url)
        if url_data["content"] is None:
            return False
        
        # iterate through words and see how many simliar phrases there are
        # n_length-gram phrase check, where n_length = 5
        for word in self.extract_words_generator(url_data).split():
            word_list.append(word.lower())
            phrase = " ".join(word_list)
            if phrase not in new_token["content"]:
                new_token["content"][phrase] = 1
            else:
                new_token["content"][phrase] += 1
            
            # if the phrase is longer than five words, remove the first word
            if len(word_list) == self.n_length:
                word_list.pop(0)
        
        # if path is a duplicate
        if parsed in self.token_dict:
            phrase_similarity_count = 0
            # ------check if content of url has similar content to another similar url----
            for new_key, new_value in new_token["content"].items():
                # if phrase in url data already in token dict and its similar, increase phrase similarity counter
                if new_key in self.token_dict[parsed]["content"]:
                    if self.token_dict[parsed]["content"][new_key] == new_value:
                        phrase_similarity_count += 1
            
            # if the content isnt similar or the url doesnt have content
            if phrase_similarity_count == 0 or max([len(self.token_dict[parsed]["content"]), len(new_token["content"])]) == 0:
                self.token_dict[parsed]["count_checks"] += 1
                if self.token_dict[parsed]["count_checks"] >= 6 and self.token_dict[parsed]["blacklist"] <= 2:
                    self.whitelist.add(parsed)
                self.token_dict[parsed]["count_checks"] += 1
                return True
            
            similarity = phrase_similarity_count / max([len(self.token_dict[parsed]["content"]), len(new_token["content"])])
            # test if just duplicate file, if so return false
            if similarity == 1:
                for item in ["index", "index.php", "php"]:
                    if item in url.split("/")[-1] or item in self.token_dict[parsed]["link"]:
                        return True
            # if very similar, remove
            if similarity > self.similarity_threshold:
                self.token_dict[parsed]["blacklist"] += 1
                self.token_dict[parsed]["count_checks"] += 1
                if self.token_dict[parsed]["blacklist"] >= 6:
                    self.blacklist.add(parsed)
                return False
            else:
                # if not that similar
                self.token_dict[parsed]["count_checks"] += 1
                if self.token_dict[parsed]["count_checks"] >= 6 and self.token_dict[parsed]["blacklist"] <= 2:
                    self.whitelist.add(parsed)
                    
        else:
            self.token_dict[parsed] = new_token
        return True

    def is_link_trap(self, url):
        """
        Checks for traps
        return False if its not a trap
        return True if it is a trap
        """
        self.is_trap = True
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        frag = parsed.fragment
        for word in ["action", "download", "upname", "session", "session_id", "sessionid", "do", "ucinetid", "format", "task", "sort", "name", "search"]:
            if word in query_params.keys():
                return True
        if url in self.check_already:
            self.is_trap = False
            return True
        
        # all fragments do is lead to part of a page -> duplicate content
        if frag != "":
            return True
            
        url_path = "/".join(parsed.path.split("/")[:-1]) 

        if url_path in self.whitelist:
            return False
        
        if url_path in self.blacklist:
            return True
        
        # not a url
        if " " in url:
            return True
        
        # reducing links based on len of the links ~79 avg len of all links
        if len(url) > 80:
            return True
        
        # reducing links through parameters
        if len(query_params) > 3:
            return True

        # check for repeating directories
        subdirectories = parsed.path.lower().split("/")
        all_subdir = [i for i in subdirectories if i != ""]
        count_sub  = [all_subdir.count(i) for i in all_subdir]
        for x in count_sub:
            if x > 1:
                return True
        try:
            if self.check_similarity("/".join(parsed.path.split("/")[:-1]), url) == False:
                self.check_already.add(url)
                return False
        except Exception as e:
            print(e)
            return False
        return True
    

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        self.is_trap = False
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            if ( ".ics.uci.edu" in parsed.hostname \
                    and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf|j_peg.php)$", parsed.path.lower())):
                pass
            else:
                return False
        except TypeError:
            #print("TypeError for ", parsed)
            return False
        
        # 
        return self.is_link_trap(url) is not True

        

