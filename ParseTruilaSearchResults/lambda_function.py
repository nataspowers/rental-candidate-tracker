import json
import requests
import bs4

def lambda_handler(event, context):
    url = 'https://www.trulia.com/for_rent/37.74735,37.89664,-122.33578,-122.19253_xy/2p_beds/2000-4000_price/1300p_sqft/bart_transit/13_zm/'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
            'Accept-Language':'en,en-US;q=0.9',
            'Accept-Encoding':'gzip, deflate, br',
            'Referer':'https://www.trulia.com/account/searches',
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Cache-Control':'max-age=0'
    }
    res = requests.get(url,headers=headers)
    res.raise_for_status()
    soup = bs4.BeautifulSoup(res.text, 'html.parser')
    cards = soup.find_all("div",{"data-hero-element-id":"srp-home-card"})
    links = soup.find_all("a")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
