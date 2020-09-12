import os
import collections
import csv
from datetime import datetime
from datetime import timedelta
import logging

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.support.ui import Select

LOGGER = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def set_up_parkrun_dir():
    if not os.path.exists('./parkrun'):
        os.mkdir('./parkrun')


def scrape_personal_results(driver, athelete_number):
    column_headings = [
        'event',
        'event_url',
        'run_date',
        'run_date_url',
        'run_number',
        'run_number_url',
        'pos',
        'time',
        'age_grade',
        'pb'
    ]

    set_up_parkrun_dir()

    previous_event_urls = []
    try:
        with open('./parkrun/personal_results.csv', 'r') as personal_result:
            race_result_reader = csv.DictReader(personal_result, fieldnames=column_headings)
            for row in race_result_reader:
                previous_event_urls.append(row['run_number_url'])
    except FileNotFoundError:
        pass

    all_results_url = 'https://www.parkrun.org.uk/results/athleteeventresultshistory/?athleteNumber={}&eventNumber=0'
    driver.get(all_results_url.format(athelete_number))
    tables = driver.find_elements_by_xpath("//table[@id='results']")
    run_rows = []
    for table in tables:
        if table.find_element_by_xpath(".//caption").text != 'All Results':
            pass
        else:
            run_rows = table.find_elements_by_xpath(".//tbody/tr[*]")
    with open('./parkrun/personal_results.csv', 'a+') as personal_results:
        results_writer = csv.writer(personal_results)
        new_results = collections.deque()
        for row in run_rows:
            run_number_url = row.find_element_by_xpath('td[3]/a').get_attribute('href')
            if run_number_url in previous_event_urls:
                continue
            data = [
                row.find_element_by_xpath('td[1]/a').text,
                row.find_element_by_xpath('td[1]/a').get_attribute('href'),
                row.find_element_by_xpath('td[2]/a').text,
                row.find_element_by_xpath('td[2]/a').get_attribute('href'),
                row.find_element_by_xpath('td[3]/a').text,
                run_number_url,
                row.find_element_by_xpath('td[4]').text,
                row.find_element_by_xpath('td[5]').text,
                row.find_element_by_xpath('td[6]').text,
                row.find_element_by_xpath('td[7]').text,
            ]
            new_results.appendleft(data)
        for data in new_results:
            results_writer.writerow(data)


def set_up_parkrun_race_results_dir():
    if not os.path.exists('./parkrun/race_results'):
        os.mkdir('./parkrun/race_results')


def close_search_tip_modal(driver):
    try:
        results_modal = driver.find_element_by_class_name('Results-modal-close')
        if results_modal:
            results_modal.click()
    except ElementNotInteractableException:
        pass


def scrape_results_for_event(driver, event_name, results_url):
    set_up_parkrun_race_results_dir()
    race_result_filename = '{}_{}.csv'.format(event_name.lower().replace(' ', ''), results_url.split('runSeqNumber=')[1])

    if os.path.exists('./parkrun/race_results/{}'.format(race_result_filename)):
        LOGGER.info('{} already exists!'.format(race_result_filename))
        return

    with open('./parkrun/race_results/{}'.format(race_result_filename), 'w+') as race_result:
        race_result_writer = csv.writer(race_result)
        driver.get(results_url)
        close_search_tip_modal(driver)
        select = Select(driver.find_element_by_name('display'))
        select.select_by_visible_text('Detailed')
        rows = driver.find_elements_by_xpath('//table/tbody/tr[*]')
        for row in rows:
            data = []
            # position
            data.append(row.find_element_by_xpath('td[1]').text)
            # Name
            try:
                data.append(row.find_element_by_xpath('td[2]/div').text)
            except NoSuchElementException:
                data.append('')
            # Name detail
            try:
                data.append(row.find_element_by_xpath('td[2]/div[2]').text)
            except NoSuchElementException:
                data.append('')
            # Gender
            try:
                data.append(row.find_element_by_xpath('td[3]/div').text)
            except NoSuchElementException:
                data.append('')
            # Gender detail
            try:
                data.append(row.find_element_by_xpath('td[3]/div[2]').text)
            except NoSuchElementException:
                data.append('')
            # Age Group
            try:
                data.append(row.find_element_by_xpath('td[4]/div').text)
            except NoSuchElementException:
                data.append('')
            # Age Group detail
            try:
                data.append(row.find_element_by_xpath('td[4]/div[2]').text)
            except NoSuchElementException:
                data.append('')
            # Club
            try:
                data.append(row.find_element_by_xpath('td[5]/div').text)
            except NoSuchElementException:
                data.append('')
            # Time
            try:
                data.append(row.find_element_by_xpath('td[6]/div[1]').text)
            except NoSuchElementException:
                data.append('')
            # Time detail
            try:
                data.append(row.find_element_by_xpath('td[6]/div[2]').text)
            except NoSuchElementException:
                data.append('')
            race_result_writer.writerow(data)


def scrape_new_race_results(driver):
    with open('./parkrun/personal_results.csv', 'r') as personal_results:
        personal_results_reader = csv.reader(personal_results)
        for result in personal_results_reader:
            race_result_filename = '{}_{}.csv'.format(result[0].lower().replace(' ', ''), result[4])
            if not os.path.exists('./parkrun/race_results/{}'.format(race_result_filename)):
                scrape_results_for_event(driver, result[0], result[3])


def scrape_all_personal_race_results(driver):
    with open('./parkrun/personal_results.csv', 'r') as personal_results:
        personal_results_reader = csv.reader(personal_results)
        for result in personal_results_reader:
            scrape_results_for_event(driver, result[0], result[3])


def scrape_all_race_results_in_period(driver, event_name, start_date, end_date):
    # Get start and end dates for year

    start_date = datetime.strptime(start_date, '%d/%m/%Y')
    end_date = datetime.strptime(end_date, '%d/%m/%Y')

    if start_date.weekday() is not 5:
        LOGGER.warning('Start date is not a Saturday, continuing...')
    if end_date.weekday() is not 5:
        LOGGER.warning('End date is not a Saturday, continuing...')

    # Get dates of each Saturday in that period

    saturdays = []
    saturday_urls = []

    # Weekday: Monday is 0 and Sunday is 6.
    d = start_date + timedelta(days=5 - start_date.weekday())
    saturdays.append(datetime.strftime(d, '%d/%m/%Y'))
    if d < start_date:
        logging.error('Back in time! {}'.format(d))
    while d < end_date:
        LOGGER.debug('Not end date yet')
        d += timedelta(days=7)
        saturdays.append(datetime.strftime(d, '%d/%m/%Y'))
    LOGGER.debug(saturdays)

    # open event results page

    driver.get('https://www.parkrun.org.uk/{}/results/eventhistory/'.format(event_name.lower().replace(' ', '')))

    # get all date elements first - quicker than searching DOM for dates
    date_elements = driver.find_elements_by_xpath('//table/tbody/tr/td/div/a')

    LOGGER.debug('Date elements: {}'.format(date_elements))

    # for saturday in saturdays:
    #     LOGGER.debug('Getting link for date {}'.format(saturday))
    for elem in date_elements:
        if elem.text in saturdays:
            LOGGER.debug('Event found for date {}'.format(elem.text))
            saturday_urls.append(elem.get_attribute('href'))

    LOGGER.debug(saturday_urls)

    for url in saturday_urls:
        scrape_results_for_event(driver, event_name, url)


scrape_all_race_results_in_period(webdriver.Firefox(), 'doddington hall', '01/01/2020', '31/12/2020')
# scrape_personal_results(driver,'1839227')
# scrape_new_race_results(driver)
