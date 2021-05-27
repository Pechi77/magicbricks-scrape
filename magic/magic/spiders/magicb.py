from json.decoder import JSONDecodeError
import os 
import json

import scrapy
import requests


class MagicbSpider(scrapy.Spider):
    handle_httpstatus_list = [404, 500]

    name = 'magicb'
    allowed_domains = ['magicbricks.com']
    # start_urls = ['https://www.magicbricks.com/property-for-sale/residential-real-estate?&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment,Residential-House,Villa&cityName=Bangalore&BudgetMin=5-Lacs&BudgetMax=40-Lacs'
    # ]
    start_urls = [
        "https://www.magicbricks.com/property-for-sale/residential-real-estate?&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment,Residential-House,Villa&cityName=Chennai"

    ]

    # start_urls = [
    #     "https://www.magicbricks.com/property-for-sale/residential-real-estate?&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment,Residential-House,Villa&cityName=Bangalore"
    # ]
    start_urls = [
        'https://www.magicbricks.com/property-for-sale/residential-real-estate?&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment&cityName=Bokaro-Steel-City'
    ]
    EMPTY_DICT = {'city_code': '',
 'page_url': '',
 'depth': '',
 'download_timeout': '',
 'download_slot': '',
 'download_latency': '',
 'url': '',
 'property_id': '',
 'Balcony': '',
 'Bedrooms': '',
 'Bathrooms': '',
 'Puja Room': '',
 'Super area': '',
 'Carpet area': '',
 'Loading': '',
 'Status': '',
 'Transaction type': '',
 'Floor': '',
 'Car parking': '',
 'Furnished status': '',
 'Type of Ownership': '',
 'Facing': '',
 'Overlooking': '',
 'city_name': '',
 'tag': '',
 'tag_price': '',
 'posted_on': '',
 'posted_name': '',
 'posted_category': '',
 'description': '',
 'Price Breakup': '',
 'Booking Amount': '',
 'Facilities': '',
 'Address': '',
 'Flooring': '',
 'Age of Construction': '',
 'Furnishing': '',
 'Brokerage Response': '',
 'Insurance Partner': '',
 'longitude': '',
 'latitude': '',
 'dev_price': '',
 'dev_rate_pr_sqft': '',
 'name': '',
 'dev_name': '',
 'dev_rating': '',
 'highlights': '',
 'locality_name': '',
 'reviews_url': '',
 'recommended_for': '',
 'overall_rating': '',
 'env_ratings': '',
 'commuting_rating': '',
 'place_of_interest_rating': '',
 'review_rating': '',
 'Environment': '',
 'env_Neighbourhood': '',
 'env_Roads': '',
 'env_Safety': '',
 'env_Cleanliness': '',
 'Commuting': '',
 '_comPublic Transport': '',
 '_comParking': '',
 '_comConnectivity': '',
 '_comTraffic': '',
 'Schools': '',
 'Restaurants': '',
 'reviews_list': '',
 'Units Available': '',
 'Authority Approval': '',
 'Configuration': '',
 'RERA ID': '',
 'Water Availability': '',
 'Loan Offered by': '',
 'Discussion Group': '',
 'Amenities': '',
 'Tower & Unit Details': '',
 'Landmarks': '',
 'Lift': '',
 'Status of Electricity': ''}

    completed_page_urls = []
    completed_urls = []
    url_to_request = start_urls.copy()[0]
    parsed = requests.utils.urlparse(url_to_request)
    query = [query.split("=") for query in parsed.query.split("&")]
    query = dict([q for q in query if len(q) > 1 and q[1]])

    headers = {
        'authority': 'www.magicbricks.com',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
        'accept': 'text/html, */*; q=0.01',
        'x-requested-with': 'XMLHttpRequest',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': start_urls[0],
        'accept-language': 'en-US,en;q=0.9,lb;q=0.8',
    }

    emtpy_set = {
        "Price Breakup",
        "Booking Amount",
        "Facilities",
        "Address" ,
        "Landmarks",
        "Facing",
        "RERA ID",
        "Overlooking",
        "Flooring",
        "Water Availability",
        "Status of Electricity",
        "Lift",
        "Age of Construction",
        "Units Available",
        "Furnishing",
        "Authority Approval",
        "Type of Ownership",
        "Loan Offered by",
        "Amenities",
        "Configuration",
        "Tower & Unit Details",
        "Status",
        "Discussion Group",
        "Floor"
    }

    def parse(self, response):
        city_code = response.xpath('//span[@id="domcache_globalNav"]/@data-citycode').get()
        #get the total number of listings available for the given city
        total_results = int(response.xpath('//span[@id="resultCount"]/text()').get())
        
        total_page = round(total_results / 30)
        self.group_start = 28
        # to test you can remove total_results and put any number, eg 5
        for page in range(1,total_page+1):
            if page > 2:
                self.group_start +=  30
            params = (
                    ('propertyType_new', '10002_10003_10021_10022,10001_10017'),
                    ('postedSince', '-1'),
                    ('city', city_code),
                    ('searchType', '1'),
                    ('propertyType', '10002,10003,10021,10022,10001,10017'),
                    ('disWeb', 'Y'),
                    ('pType', '10002,10003,10021,10022,10001,10017'),
                    ('category', 'S'),
                    # ('budgetMax', '4000000'),
                    # ('budgetMin', '500000'),
                    ('cusImgCount', '1'),
                    ('groupstart', 0 if page==1 else self.group_start),
                    ('offset', '0'),
                    ('maxOffset', '55'),
                    ('attractiveIds', ''),
                    ('page', page),
                    ('ltrIds', ''),
                    ('preCompiledProp', ''),
                    ('excludePropIds', ''),
                    ('addpropertyDataSet', ''),
            )

            # new_params = {
            #         **self.query,
            #         "city":city_code,
            #         "page":page
            # }

            if "budgetMax" in self.start_urls[0]:
                dict_params = dict(params)
                budget_params = {
                            "budgetMax" : self.query.get("budgetMax",""),
                            "budgetMin" : self.query.get("budgetMin",""),
                }
                params = {
                    **dict_params,
                    **budget_params
                }
            prepared = requests.PreparedRequest()
            prepared.prepare(url='https://www.magicbricks.com/mbsearch/propertySearch.html',params=params)
            page_url = prepared.url
            
            meta = {"city_code":city_code,
                    "page_url":page_url}
            
            os.makedirs("links2",exist_ok=True)

            if "page=1" in page_url and page_url not in self.completed_page_urls:
                self.completed_page_urls.append(page_url)
                with open(f"links2/{page}.txt","w") as f:
                    f.write(page_url)
                
                yield scrapy.Request(self.start_urls[0],
                                dont_filter=True,
                                meta=meta.copy(),
                                callback=self.scrape_listings
                    ) 
            
            else:
                if page_url not in self.completed_page_urls:
                    self.completed_page_urls.append(page_url)
                    with open(f"links2/{page}.txt","w") as f:
                        f.write(page_url)
                    yield scrapy.Request(page_url,
                                        dont_filter=True,
                                    meta=meta.copy(),
                                    callback=self.scrape_listings
                        )  
    
    def scrape_listings(self,response):
        
        meta = response.meta.copy()
        listings = response.xpath('//div[contains(@class,"m-srp-card__container")]')
        
        urls2 = []
        for idx, listing in enumerate(listings):
            if listing.xpath('.//a[contains(@id,"project-link-")]/@id').get():
                property_id = listing.xpath('.//a[contains(@id,"project-link-")]/@id').get().split("-")[-1]
            elif listing.xpath('.//div[contains(@id,"no_image_")]/@id').get():
                property_id = listing.xpath('.//div[contains(@id,"no_image_")]/@id').get().split("_")[-1]
            elif listing.xpath('.//div[contains(@id,"imageCountProperty")]/@id').get():
                property_id = listing.xpath('.//div[contains(@id,"imageCountProperty")]/@id').get().strip("imageCountProperty")
            else:
                
                print(f"unable to find property id for {idx}")
                
                    
            url = response.xpath(f'//span[@id="domcache_srp_{property_id}"]/@data-detailurl').get()
            urls2.append((url,property_id))
        breakpoint()

        for url,property_id_ in urls2:

            url_meta = {**meta.copy(),
                    "url":url,
                    "property_id" : property_id_
                    }
                
            
            if url not in self.completed_urls and url is not None:
                
                self.completed_urls.append(url)
                
                yield scrapy.Request(url,
                                    dont_filter=True,
                                    meta=url_meta.copy(),
                                    callback=self.scrape_page

                )
    

    def scrape_page(self,response):
        # if response.url == "https://www.magicbricks.com/propertyDetails/1-BHK-493-Sq-ft-Multistorey-Apartment-FOR-Sale-Nagarbhavi-in-Bangalore&id=4d423335353933363139":
        #     breakpoint()
        if response.status != 200:
            
            yield {
                   **self.EMPTY_DICT,
                   **response.meta.copy()
            }


        title_preprocess = {
            "Bedroom" : "Bedrooms",
            "Bathroom" : "Bathrooms"
        }
        meta = response.meta.copy()
        meta["Balcony"] = ""

        
        ### PROPERTY LEVEL DATA
        posted_on = response.xpath('//div[@class="postedOn"]/text()').get().split(":")[-1].strip()
        try:
            posted_name = response.xpath('//div[@class="nameValue"]/text()').get().strip()                
            posted_category = response.xpath('//div[@class="nameTitle"]/text()').get()
        except AttributeError:
            posted_name = response.xpath('//div[@class="CA_name_Detail"]/div[@class="CAName"]/text()').get().strip()
            posted_category = response.xpath('//div[@class="CABatch"]/text()').get().strip()

        tag = "".join(response.xpath('//span[@class="p_bhk"]/text()').getall()).strip()
        infos = response.xpath('//div[@class="p_infoColumn"]')
        tag_price = response.xpath('//div[@class="priceSqft"]/div[@class="p_price"]/div/meta[@itemprop="price"]/@content').get() or ""
        
        city_name = response.xpath('//span[@id="domcache_detailpage"]/@data-city').get() or ""

        property_details = {}

        for info in infos:
            title = info.xpath('.//div[@class="p_title"]/text()').get().strip().replace("\t","").replace("\n","").strip()
            value = "".join(info.xpath('.//div[contains(@class,"p_value")]//text()').getall()).strip().replace("\t","").replace("\n","")
            title = title_preprocess.get(title,title)
            if title.endswith("area"):
                value = value.split(" ")[0]
            property_details[title] = value

        property_details["city_name"] = city_name
        property_details["tag"] = tag
        property_details["tag_price"] = tag_price
        property_details["posted_on"] = posted_on
        property_details["posted_name"] = posted_name
        property_details["posted_category"] = posted_category
        

        ### DESCRIPTION LEVEL DATA
        array_keys = ["Amenities","Facilities"]
        # if response.url == "https://www.magicbricks.com/propertyDetails/2-BHK-943-Sq-ft-Multistorey-Apartment-FOR-Sale-Sarjapur-Road-in-Bangalore&id=4d423534333536393835":
            
        description = "\n".join(response.xpath('//div[@id="prop-detail-desc"]//text()').getall()).strip()
        if not description:
            if response.xpath('//div[@class="descriptionCont"]//div[@class="p_infoRow"]/br'):
                description = "\n".join(response.xpath('//div[@class="descriptionCont"]//div[@class="p_infoRow"][1]/text()').getall()).replace("\n","").strip()
                
            else:
                description = response.xpath('//div[@class="descriptionCont"]//div[@class="p_infoRow"]/text()').get().strip()


        description_dict = {"description":description}

        for info in response.xpath("//div[@class='descriptionCont']//div[contains(@class,'p_infoRow')]"):
            if info.xpath('.//div[@class="p_title"]/text()').get():
                title = info.xpath('.//div[@class="p_title"]/text()').get().strip().replace("  ","").replace("\t","").replace("\n","").strip()
                if title in array_keys:
                    value = ",".join(info.xpath('.//div[@class="p_value"]//text()').getall()).strip().replace("  ","").replace("\t","").replace("\n","").strip().replace(",,",",").strip(",").split(",")
                    value = list(filter(None,value))
                    value = str({title : list(map(str.strip,value))})
                else:
                    value = "".join(info.xpath('.//div[@class="p_value"]//text()').getall()).strip().replace("  ","").replace("\t","").replace("\n","").strip()
                    
                if title.endswith("area"):
                    value = value.split(" ")[0]
                description_dict[title] = value
        if description_dict.get("Landmarks"):
            description_dict["Landmarks"] = str({"Landmarks":description_dict["Landmarks"].split(",")})
        
        ###LAT LONG
        titles = response.xpath('//div[@itemprop="geo"]/meta/@itemprop').getall()
        values = response.xpath('//div[@itemprop="geo"]/meta/@content').getall()
        lat_long = dict(zip(titles,values))

        ### PROJECT DETAILS
   
        name = response.xpath('//div[@class="projName"]/a/text()').get()
        try:
            dev_name = response.xpath('//div[@class="devName"]/a/text()').get().strip()
        except:
            dev_name = "Owner"
        rating = response.xpath('//span[@class="proj-rating__number"]/text()').get()
        details = response.xpath('//div[@class="detailsRow"]/div[contains(@class,"column col_")]')
        details_dict = {}
        for detail in details:
            title = detail.xpath('.//div[@class="detailsLabel"]/text()').get().strip().replace("\t","").replace("\n","")
            value = "".join(detail.xpath('.//div[@class="detailsVal"]//text()').getall()).strip().replace("\t","").replace("\n","")
            details_dict[title] = value

        # try:
        
        # breakpoint()
        # except:
        
       

        dev_price = response.xpath('//div[@class="detailsRow"]/div[@class="column col_1"]/div[@class="detailsVal"]/div[@class="semiBold"]/text()').get() or ""
        dev_rate_per_sqft = response.xpath('//div[@class="detailsRow"]/div[@class="column col_1"]/div[@class="detailsVal"]/div[@class="openSans_regular fsize_12"]/text()').get() or ""

        
        details_dict["dev_price"] = dev_price
        details_dict["dev_rate_pr_sqft"] = dev_rate_per_sqft
        details_dict["name"] = name
        details_dict["dev_name"] = dev_name
        details_dict["dev_rating"] = rating

        highlights = response.xpath('//section[@class="detailsHighLight"]//ul/li/text()').getall()  
        details_dict["highlights"] = highlights

        ### LOCALITY DETAILS
        locality_name = response.xpath('//div[@class="ratePhotoHeading"]/text()').get() or ""
        details_dict["locality_name"] = locality_name
        mbrick_key = response.xpath('//span[@id="domcache_detailpage"]/@data-mbratingkey').get()
        explore_city_url = response.xpath('//div[@class="propBhk"]//h1//span[@class="p_text"]//span[@class="p_address"]/a/@href').get()
        local_code = response.xpath('//span[@id="domcache_detailpage"]/@data-localityid').get()
        params = (
            ('callback', 'newDetailsRatingAndReviewWidget'),
        #     ('parentData', '/{"resultDiv":"rating","exploreLocalityUrl":"https://www.magicbricks.com/mysore-road-in-bangalore-Overview#","agentLink":"${agentContextRoot}userProfileListing.html?oid=","modelAreaDivId":"modelWindowArea","localityName":"Mysore Road","cityName":"Bangalore","extTest":""/}'),
            ('host', 'magicbricks.com'),
            ('key', mbrick_key),
            ('code', local_code),
            ('mbRatingUserRefNum', '-1'),
            ('lrr', '1'),
            ('oag', '1'),
            ('tvm', '0'),
            ('lrrl', '1'),
            ('type', 'LOCALITY'),
        )

        prepared = requests.PreparedRequest()
        prepared.prepare(url='https://rating.magicbricks.com/mbRating/getWiget.json',params=params)
        json_url = prepared.url

        json_meta = {
            **meta,
            **property_details,
            **description_dict,
            **lat_long,
            **details_dict
        }

        yield scrapy.Request(json_url,
                            dont_filter=True,
                            callback=self.extract_json_data,
                            meta=json_meta.copy()
                )   
        
    def extract_json_data(self, response):
        if response.status != 200:
            
            yield {
                   **self.EMPTY_DICT,
                   **response.meta.copy()
            }

        meta = response.meta.copy()
        start = response.text.find("(") + 1
        json_data = {}
        try:
            json_data = json.loads(response.text[start:-2])
        except JSONDecodeError:
            yield {**self.EMPTY_DICT,
                    **meta.copy()
                    }

        recommended_for = json_data.get('recommendedFor',"") or ""
        overall_rating = json_data.get('avgRating',"") or ""
        try:
            env_ratings =  json_data.get('categoryRatingMap',{}).get('Environment',"")
        except:
            env_ratings = ""
        try:
            commuting_rating = json_data.get('categoryRatingMap',{}).get('Commuting',"")
        except:
            commuting_rating = ""
        try:
            place_of_interest_rating = json_data.get('categoryRatingMap',{}).get('Places of Interest',"")
        except:
            place_of_interest_rating = ""

        json_dict = {}
        json_dict["recommended_for"] = recommended_for
        json_dict["overall_rating"] = overall_rating
        json_dict["env_ratings"] = env_ratings
        json_dict["commuting_rating"] = commuting_rating
        json_dict["place_of_interest_rating"] = place_of_interest_rating

        url = meta["url"]
        review_url = url.split("&")[0]
        place = review_url.split("-Sale-")[1]
        reviews_url = f"https://www.magicbricks.com/real-estate-property-reviews/{place}#reviewResult"
        meta["reviews_url"] = reviews_url

        another_meta = {
            **meta,
            **json_dict
        }

        yield scrapy.Request(reviews_url,
                        dont_filter=True,
                        meta=another_meta.copy(),
                        callback=self.scrape_reviews
                    )
    
    def scrape_reviews(self, response):
        if response.status != 200:
            
            yield {
                   **self.EMPTY_DICT,
                   **response.meta.copy()
            }
            

        meta = response.meta.copy()

        star_map = {
            "full" : 1,
            "half" : .5
        }
        review_title_map = {
            "Environment" : "env_",
            "Commuting" : "_com",
            "Places of Interest":"poi_"
        }
        overall_rating = response.xpath('//span[@class="loc-det-rev__starvalue"]/text()').get()
        
        review_blocks = response.xpath('//div[@class="loc-det-rev__rateblocks"]/div[@class="loc-det-rev__rateblock"]')
        review_ratings_dict = {"review_rating":overall_rating}

        for review_block in review_blocks[:-1]:
            title = review_block.xpath('.//div[@class="loc-det-rev__rateblock__title"]/text()').get()
            if review_block.xpath('.//div[@class="loc-det-rev__rateblock__value"]/text()').get():
                rating = review_block.xpath('.//div[@class="loc-det-rev__rateblock__value"]/text()').get().split("/")[0]
            else:
                rating = ""
            review_ratings_dict[title] = rating
            sub_blocks = review_block.xpath('.//div[@class="loc-det-rev__rateblock__details"]//div[@class="loc-det-rev__rateblock__td"]')
            
            for idx, sub_block in enumerate(sub_blocks):
                if idx % 2 == 0:
                    sub_title = sub_block.xpath('.//text()').get()
                else:
                    stars = sub_block.xpath('.//span[@class="loc-det-rev__rateblock__stars"]/span[contains(@class,"loc-det__blocks__smstar")]/@class').getall()
                    stars = sum([star_map.get(star.split()[-1],0) for star in stars])
                    review_ratings_dict[review_title_map.get(title,"new")+sub_title] = stars
        try:
            sub_blocks = review_blocks[-1].xpath('.//div[@class="loc-det-rev__rateblock__td"]')
        except IndexError:
            sub_blocks = []

        title = ""

        for idx, sub_block in enumerate(sub_blocks):
            if sub_block.xpath('.//text()').get().strip():
                if not title:
                    title = sub_block.xpath('.//text()').get().strip()
                else:
                    value = sub_block.xpath('.//text()').get().strip()
                    review_ratings_dict[title] = value
                    title = ""
        
        reviews_list = []
        reviews = response.xpath('//div[@class="loc-det-rev__reviewlist"]')
        for review in reviews:
            reviewer_name = review.xpath('.//div[@class="loc-det__livablityblock__username-name"]/text()').get()
            category = review.xpath('.//div[@class="loc-det-rev__rtitle__type"]/text()').get()
            review_stars = review.xpath('.//span[@class="loc-det-rev__rtitle__rating"]/span[contains(@class,"loc-det__blocks__smstar")]/@class').getall()
            review_star = sum(star_map.get(review_star.split()[-1],0) for review_star in review_stars)
            review_title = review.xpath('.//div[@class="loc-det-rev__rtitle__heading"]/text()').get()
            review = review.xpath('.//div[@class="loc-det-rev__rtitle__localtxt addReadMore showlesscontent"]/text()').get()
            
            reviews_list.append(
                {
                    "name" : reviewer_name,
                    "category":category,
                    "rating" : review_star,
                    "title" : review_title,
                    "review" : review
                }
            )
        review_ratings_dict["reviews_list"] = reviews_list
        final_data = {
                **meta.copy(),
                **review_ratings_dict
            }

        if final_data.get("highlights"):
            if isinstance(final_data["highlights"],list):
                final_data["highlights"] = str({"highlights":final_data["highlights"]})    
            else:
                final_data["highlights"] = str({"highlights":final_data["highlights"].split(",")})

        for key in self.emtpy_set:
            if key not in final_data:
                final_data[key] = ""

        yield final_data




            


