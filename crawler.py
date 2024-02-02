import logging
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, urljoin, parse_qsl, urlunparse
from lxml import html, etree
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
        self.subdomains = dict()
        self.blacklist = set()
        self.whitelist = set()
        self.check_already = set()
        self.similarity_threshold = 0.9
        self.n_length = 5
        self.token_dict = {}
        self.vocabulary = {}
        self.stop_words = []
        self.page_most_links = {
            "link": "",
            "count": 0
        }
        self.longest_page = {
            "link": "",
            "count": 0
        }
        self.traps = set()
        self.downloaded = set()
        self.create_stop_words()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        
        Path.touch("downloaded.txt")
        with Path.open("downloaded.txt", "a", encoding="utf-8") as txt:
            while self.frontier.has_next_url():
                url = self.frontier.get_next_url()
                logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
                self.whitelist.add(url)
                url_data = self.corpus.fetch_url(url)
                subdomain = self.analytics(url_data, url)
                if subdomain not in self.subdomains:
                    self.subdomains[subdomain] = set()
                valid_links_counter = 0
                self.downloaded.add(url)
                # Write links to txt
                txt.write(url + "\n")
                for next_link in set(self.extract_next_links(url_data)):
                    if self.is_valid(next_link):
                        if self.corpus.get_file_name(next_link) is not None:
                            self.frontier.add_url(next_link)
                            self.subdomains[subdomain].add(next_link)
                            valid_links_counter += 1
                    else:
                        self.traps.add(next_link)
                # update link with the most valid links out
                if valid_links_counter > self.page_most_links["count"]:
                    self.page_most_links = {
                        "link": url,
                        "count": valid_links_counter
                    }

    def write_analytics_to_file(self):
        Path.touch("traps.txt")
        with Path.open("traps.txt", "a", encoding="utf-8") as trap_file:
            for i in self.traps:
                trap_file.write(i + "\n")
        # subdomain analytics
        Path.touch("subdomains.txt")
        with Path.open("subdomains.txt", "a", encoding="utf-8") as subdomains_file:
            subdomains_file.write("subdomains : count of different urls\n")
            for key, value in self.subdomains.items():
                subdomains_file.write(f"{key} : { len(value)}")
        # Path.touch("stats.txt")
        sorted_vocabulary = dict(sorted(self.vocabulary.items(), key= lambda x: x[1], reverse=True))
        with Path.open("stats.txt", "a", encoding="utf-8") as stats_file:
            stats_file.write(f"longest page: {self.longest_page['link']} \n count: {self.longest_page['count']}\n")
            stats_file.write("50 most common words\n")
            counter = 1
            for key, value in sorted_vocabulary.items():
                stats_file.write(f"{counter}. {key} : {value}\n")
                if counter == 51:
                    break
                else:
                    counter += 1

    def analytics(self, url_data, url):
        """ Does the analytics"""
        all_text = self.extract_words_generator(url_data)
        subdomain = urlparse(url_data["url"]).netloc
        if subdomain not in self.subdomains:
            self.subdomains[subdomain] = set()
        if len(all_text.split()) > self.longest_page["count"]:
            self.longest_page = {
                "link": url,
                "count": len(all_text.split())
            }
        list_text = all_text.lower().split()
        unique_words = set(list_text)
        for word in self.stop_words:
            if word.lower() in unique_words:
                unique_words.discard(word.lower())
        for word in unique_words:
            if word in self.vocabulary:
                self.vocabulary[word] += list_text.count(word)
            else:
                self.vocabulary[word] = list_text.count(word)
        return subdomain
    
    def create_stop_words(self):
        with open("stopwords.txt", "r") as stop_words_file:
            self.stop_words = stop_words_file.readlines()
        self.stop_words = [ x.strip().lower() for x in self.stop_words]
        
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

        if url is not None and content is not None:
            # parses html content
            try:
                tree = html.fromstring(content)
                # gets all relative and absolute links
                all_links = tree.xpath("//a/@href")
                # turns every relative link into absolute
                absolute_urls =[urljoin(url, link) for link in all_links]
                return absolute_urls
            except (etree.ParserError):
                return []
        else:
            return []
    
    def count_num(self, s):
        num = 0
        for char in s:
            if char.isdigit():
                num += 1
        return num

    def is_valid_path(self, parsed_url):
        """
        where parsed_url = urlparse(url) object
        """
        # if there is not path
        if parsed_url.path == "" or parsed_url.path == '/':
            return True
        
        # gets url pieces w/o query and fragment
        new_url_tuple = (parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', '')

        # puts those url pieces together
        clean_url = urlunparse(new_url_tuple)
        clean_url = urlparse(clean_url)

        # get path
        folder = ''
        file = ''
        path = clean_url.path
        path_lst = path.split('/')
        clean_path_lst = [i for i in path_lst if (i != "")]
        last_path = clean_path_lst[len(clean_path_lst) - 1]
        # get file and folder from path
        if "." in last_path:
            if len(clean_path_lst) > 1:
                folder = clean_path_lst[-2]
                file = last_path.split('.')[0]
            else:
                file = last_path.split('.')[0]
                folder = ""
        else:
            file = ""
        
        # check folder
        if folder != "" and file == "":
            if folder.isalnum():
                num = self.count_num(folder)
                if num / len(folder) >= 0.5:
                    return False
                else:
                    return True
            return False
        if folder != "" and file != "":
            if folder.isalnum():
                num = self.count_num(folder)
                if num / len(folder) >= 0.5:
                    return False
                else:
                    num = self.count_num(file)
                    if num / len(file) >= 0.5:
                        return False
                    else:
                        return True
            return False
        return False

    def extract_words_generator(self, url_data):
        """
        Function that gets the file content. It grabs the anything in the p, h1, h2, h3, h4, h5, h6, span and div tabs. (Tabs that generally include words)
        This function returns the list of words in that file.
        """
        return BeautifulSoup(url_data["content"]).get_text()
    
    def check_simlarity(self, parsed, url):
        """
        Tokenize the words and htei frequency. Checks if they are in the dictionary, if not hten add. If it is found, do a similarity test and update the 
        black and waitlist
        """
        new_token = {
                "content": {},
                "blacklist": 0,
                "count_checks": 1,
                "link": url
            }
        word_list = []
        phrase = ""
        url_data = self.corpus.fetch_url(url)
        if url_data["content"] is None:
            return False
        # iterate through words and see how many simliar phrases there are
        for word in self.extract_words_generator(url_data).split():
            word_list.append(word.lower())
            phrase = " ".join(word_list)
            if phrase not in new_token["content"]:
                new_token["content"][phrase] = 1
            else:
                new_token["content"][phrase] += 1
            if len(word_list) == self.n_length:
                word_list.pop(0)
        # if exntry exists already 
        if parsed in self.token_dict:
            count = 0
            for new_key, new_value in new_token["content"].items():
                # if path already in token dict, check similarity
                if new_key in self.token_dict[parsed]["content"]:
                    if self.token_dict[parsed]["content"][new_key] == new_value:
                        count += 1
            if count == 0 or max([len(self.token_dict[parsed]["content"]), len(new_token["content"])]) == 0:
                self.token_dict[parsed]["count_checks"] += 1
                if self.token_dict[parsed]["count_checks"] == 10 and self.token_dict[parsed]["blacklist"] == 2:
                    self.whitelist.add(parsed)
            similarity = count / max([len(self.token_dict[parsed]["content"]), len(new_token["content"])])
            # test if just duplicate file, if so return false
            if similarity == 1:
                for item in ["index", "index.php", "php"]:
                    if item in url.split("/")[-1] or item in self.token_dict[parsed]["link"]:
                        return True
            # if very similar, remove
            if similarity > self.similarity_threshold:
                with Path.open("removed.txt", "a", encoding="utf-8") as removed:
                    removed.write(f"{url} --Duplication score: {similarity}\n")
                self.token_dict[parsed]["blacklist"] += 1
                if self.token_dict[parsed]["blacklist"] == 6:
                    self.blacklist.add(parsed)
                return False
            else:
                self.token_dict[parsed]["count_checks"] += 1
                if self.token_dict[parsed]["count_checks"] == 10 and self.token_dict[parsed]["blacklist"] == 2:
                    self.whitelist.add(parsed)
        else:
            self.token_dict[parsed] = new_token
        return True

        
    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        query_params = parse_qsl(parsed.query)
        if parsed.scheme not in set(["http", "https"]):
            return False
        # slug = parsed.
        try:
            if ( ".ics.uci.edu" in parsed.hostname \
                   and not re.match(r".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())):
                pass
            else:
                return False
            # not a url
            if " " in url:
                return False
            # reducing links based on len of the links ~79 avg len of all links
            if len(url) > 80:
                return False
            # reducing links through parameters
            if len(query_params) > 5:
                return False
            # if not self.is_valid_path(parsed):
            #     return False
            # reducing links based on url fragments
            if parsed.fragment in ["content-main"]:
                return False
            if "id=" in parsed.path.split('/')[-1]:
                return False
            # check for repeating sub directoried
            list_dir = parsed.path.split("/")
            for item in list_dir:
                if list_dir.count(item) > 2:
                    return False
            if len(list_dir) > 10:
                return False
            # if it keeps getting longer then prob trap
            next_url = url
            next_len = len(url)
            for i in range (5):
                next_url_data = self.corpus.fetch_url(next_url)
                if next_url_data["content"] is None:
                    break
                next_possible_links = self.extract_next_links(next_url_data)
                if len(next_possible_links) == 0:
                    break
                next_url = max(next_possible_links, key= lambda x: len(x))
                if len(next_url) >= next_len:
                    next_len = len(next_url)
                else:
                    break
            if "/".join(parsed.path.split("/")[:-1]) in self.whitelist:
                return True
            if "/".join(parsed.path.split("/")[:-1]) in self.blacklist:
                return False
            if parsed.path in self.check_already:
                return False
            if self.check_simlarity("/".join(parsed.path.split("/")[:-1]), url) == False:
                self.check_already.add(parsed.path)
                return False
            return True
        except TypeError:
            #print("TypeError for ", parsed)
            return False
        

