import os
import sys
import json
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.keys import Keys


with open("categories.json", "rb") as file:
  categories = json.loads(file.read())


authorized = False
domain = "https://www.zhipin.com/"
driver = webdriver.Chrome()
results = []


for category in categories:
  request_url = domain + category["url"]
  driver.get(request_url)
  driver.implicitly_wait(10)

  if not authorized:
    print("请在弹出的浏览器页面中登录，然后复制以下地址到浏览器中打开，然后按任意键继续：\n%s" % request_url)
    os.system("pause")
    authorized = True
  
  count_of_current = 0
  page_of_current = 1

  job_category = category["type"]
  job_sub_category = category["name"]
  print("当前抓取的分类: %s/%s" % (job_category, job_sub_category))

  while page_of_current < 15:
    print("抓取第 %d 页.." % page_of_current)
    page_of_current += 1
 
    job_items = driver.find_elements_by_css_selector('.job-primary')
    for item in job_items:
      try:
        job_name = item.find_element_by_css_selector('.job-name a').text
        job_area = item.find_element_by_css_selector('.job-area').text
        
        salary_range = item.find_element_by_css_selector('.job-limit .red').text.replace('K', '')
        annual_bonus = None
        if salary_range.find("·") != -1:
          annual_bonus = salary_range.split('·')[1]
          salary_range = salary_range.split('·')[0]
        min_salary = int(salary_range.split('-')[0])
        max_salary = int(salary_range.split('-')[1])

        limit = item.find_element_by_css_selector('.job-limit.clearfix > p').get_attribute("innerHTML").split("<em class=\"vline\"></em>")
        experience_requirement = limit[0]
        education_requirement = limit[1]

        company_name = item.find_element_by_css_selector('.company-text .name a').text
        company_detail = item.find_element_by_css_selector('.company-text > p').get_attribute("innerHTML").split("<em class=\"vline\"></em>")
        company_category = item.find_element_by_css_selector('.company-text .false-link').text
        company_scale = company_detail[len(company_detail) - 1]
        company_welfare = item.find_element_by_css_selector('.info-desc').text

        tags = item.find_elements_by_css_selector('.tag-item')
        keywords = ""
        for tag in tags:
          keywords = keywords + "#" + tag.text

        count_of_current += 1
        results.append({
          "job_category": job_category,
          "job_sub_category": job_sub_category,
          "job_name": job_name,
          "job_area": job_area,
          "min_salary": min_salary,
          "max_salary": max_salary,
          "annual_bonus": annual_bonus,
          "experience_requirement": experience_requirement,
          "education_requirement": education_requirement,
          "company_name": company_name,
          "company_category": company_category,
          "company_scale": company_scale,
          "company_welfare": company_welfare,
          "keywords": keywords
        })
      except Exception as err:
        print("[分类 %s 页面 %d] Exception: " % job_category, page_of_current, err)

    try:
      has_next_page = driver.find_element_by_css_selector('.next:not(.disabled)')      
      if has_next_page:
        driver.get(request_url + "?page=%d" % (page_of_current))
    except Exception:
      print("分类【%s】爬取完成。正在保存临时结果到 dataset.csv" % job_sub_category)
      
      dataframe = pd.DataFrame(results)
      dataframe.to_csv("dataset.csv")

      break

dataframe = pd.DataFrame(results)
dataframe.to_csv("dataset.csv")

print("结果已保存到 ./dataset.csv")
