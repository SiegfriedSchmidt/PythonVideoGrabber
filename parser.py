from bs4 import BeautifulSoup as Soup
import requests

content = requests.get('https://player02.getcourse.ru/player/ad756aefad20267eb5e266cd1c8c03ea/11a93871228bebbdf94ea19b61ec022e/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmq')
print(content.text)
