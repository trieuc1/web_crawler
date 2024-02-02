import logging
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
        self.token_dict = {}
        self.blacklist = set()
        self.whitelist = set()
        self.similarity_threshold = 0.90
        self.n_length = 5
        self.stop_words = []
        self.page_most_links = {"link": "", "count": 0}
        self.subdomains = {}
        self.is_trap = False
        self.check_already = set()
        self.create_stop_words()
        print(self.stop_words)

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
            valid_links_counter = 0
            # Write links downloaded.txt
            with Path.open("downloaded.txt", "a", encoding="utf-8") as txt:
                txt.write(url + "\n")
            for next_link in set(self.extract_next_links(url_data)):
                validty = self.is_valid(next_link)
                if self.is_trap == False and validty == False:
                    pass
                elif validty:
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        valid_links_counter += 1
                else:
                    with Path.open("removed.txt", "a", encoding="utf-8") as removed:
                        removed.write(next_link + "\n")
            # update link with the most valid links out
            if valid_links_counter > self.page_most_links["count"]:
                self.page_most_links = {
                    "link": url,
                    "count": valid_links_counter
                }
    
    def create_stop_words(self):
        with open("stopwords.txt", "r") as stop_words_file:
            self.stop_words = stop_words_file.readlines()
        self.stop_words = [ x.strip().lower() for x in self.stop_words]
    def run_analytics(self):
        """
        Run the analytics after seperating all the crawler traps out
        """
        longest_page = {"link": "", "count": 0}
        vocabulary = {}
        # Analytics #2
        with open("analytics.txt", "a") as file:
            file.write(f"Link with the most valid out links:\n{self.page_most_links}\n")
        for link in self.frontier.urls_set:
            url_data = self.corpus.fetch_url(link)
            url_text = self.extract_words_generator(url_data).lower().split()
            url_text_length = len(url_text)
            # Analytics #1
            subdomain = urlparse(link).netloc
            if subdomain not in self.subdomains:
                self.subdomains[subdomain] = 0
            self.subdomains[subdomain] += 1
            # Analytics # 4
            if url_text_length > longest_page["count"]:
                longest_page = {
                    "link": link,
                    "count": url_text_length
                }
            url_text_set = set(url_text)
            for word in self.stop_words:
                url_text_set.discard(word)
            for word in url_text_set:
                if word not in vocabulary:
                    vocabulary[word] = 0
                vocabulary[word] += url_text.count(word)
        with open("analytics.txt", "a", encoding="utf-8") as file:
            # Analytics 1
            file.write(f"\n\nSubdomains: Links proccessed\n")
            for subdomain, count in self.subdomains.items():
                file.write (f"{subdomain}: {count}\n")
            # Analytics 4
            file.write(f"\n\nLongest Page: \n{longest_page}\n")
            # Analytics 5
            sorted_vocab = dict(sorted(vocabulary.items(), key=lambda x: x[1], reverse=True))
            file.write("\n\n50 most common words:\n")
            counter = 1
            for word, count in sorted_vocab.items():
                if counter == 51:
                    break
                file.write(f"{counter}. {word}: {count}\n")
                counter += 1
        
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

        if url is not None and content is not None and content is not "" and len(content) != 0:
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
                self.token_dict[parsed]["count_checks"] += 1
                return True
            similarity = count / max([len(self.token_dict[parsed]["content"]), len(new_token["content"])])
            # test if just duplicate file, if so return false
            if similarity == 1:
                for item in ["index", "index.php", "php"]:
                    if item in url.split("/")[-1] or item in self.token_dict[parsed]["link"]:
                        return True
            # if very similar, remove
            if similarity > self.similarity_threshold:
                self.token_dict[parsed]["blacklist"] += 1
                self.token_dict[parsed]["count_checks"] += 1
                if self.token_dict[parsed]["blacklist"] == 10:
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
        self.is_trap = False
        try:
            if ( ".ics.uci.edu" in parsed.hostname \
                    and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())):
                pass
            else:
                return False
        except TypeError:
            #print("TypeError for ", parsed)
            return False
        status = True
        self.is_trap = True
        if parsed.scheme not in set(["http", "https"]):
            status = False
        # not a url
        if " " in url:
            status = False
        # reducing links based on len of the links ~79 avg len of all links
        if len(url) > 80:
            status = False
        # reducing links through parameters
        if len(query_params) > 5:
            status = False
        # if not self.is_valid_path(parsed):
        #     print("valid")
        #     status = False
        # reducing links based on url fragments
        if parsed.fragment in ["content-main"]:
            status = False
        for word in ["netid=", "session=", "session_id=", "sessionid="]:
            if word.strip("?") in parsed.path.split('/'):
                status = False
        subdirectories = parsed.path.lower().split("/")
        for subdir in set(subdirectories):
            if subdirectories.count(subdir) > 3:
                status = False
        if parsed.query(subdir) > 3:
                status = False
        if len(urlparse.parse_qs(parsed.query)) > 3:
            status = False
        if status == False:
            return False
        
        if url in self.check_already:
            self.is_trap = False
            return False
        url_path = "/".join(parsed.path.split("/")[:-1]) 
        if url_path in self.whitelist:
            return True
        if url_path in self.blacklist:
            return False
        try:
            if self.check_simlarity("/".join(parsed.path.split("/")[:-1]), url) == False:
                self.check_already.add(url)
                return False
        except Exception as e:
            print(e)
        return True
        

