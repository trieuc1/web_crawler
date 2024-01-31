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
        self.blacklist = []
        self.similarity_threshold = 0.90

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        
        Path.touch("url_links.txt")
        Path.touch("removed.txt")
        with Path.open("url_links.txt", "a", encoding="utf-8") as txt:
            while self.frontier.has_next_url():
                url = self.frontier.get_next_url()
                #logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))

                url_data = self.corpus.fetch_url(url)
                # Write links to txt
                txt.write(url + "\n")
                for next_link in self.extract_next_links(url_data):
                    if self.is_valid(next_link, url_data):
                        if self.corpus.get_file_name(next_link) is not None:
                            self.frontier.add_url(next_link)

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
        if parsed_url.path == "":
            print("eller")
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
        print(clean_path_lst)
        print(last_path)
        # get file and folder from path
        if "." in last_path:
            if len(clean_path_lst) > 1:
                folder = clean_path_lst[-2]
                file = last_path.split('.')[0]
                print(folder)
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
            print("up")
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
            

    def is_valid(self, url, url_data):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        # slug = parsed.
        query_params = parse_qsl(parsed.query)
        if parsed.scheme not in set(["http", "https"]):
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
        if not self.is_valid_path(parsed):
            return False
        # reducing links based on url fragments
        if parsed.fragment in ["content-main"]:
            return False
        if "id=" in parsed.path.split('/')[-1]:
            return False
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
            if url in self.blacklist:
                return False
            new_token = {
                "content": {}
            }
            for word in self.extract_words_generator(url_data):
                if word not in new_token["content"]:
                    new_token["content"][word] = 1
                else:
                    new_token["content"][word] += 1
            if parsed.path in self.token_dict:
                for token in self.token_dict[parsed.path]:
                    # if path already in token dict, check similarity
                    count = 0
                    for key, value in token["content"].items():
                        if key in new_token["content"]:
                            if new_token["content"][key] == value:
                                count += 1
                    similarity = count / len(token["content"])
                    if similarity > self.similarity_threshold:
                        with Path.open("removed.txt", "a", encoding="utf-8") as removed:
                            removed.write(url + "\n")
                        self.blacklist.append(url)
                        return False
                self.token_dict[parsed.path].append(new_token)
            else:
                self.token_dict[parsed.path] = []
                self.token_dict[parsed.path].append(new_token)
            return True
        except TypeError:
            #print("TypeError for ", parsed)
            return False
        

