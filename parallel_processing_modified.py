"""This example demonstrates how to asynchronously fetch product prices across websites in parallel with query_data() method."""

import os
import asyncio
import pandas as pd

from agentql.tools.async_api import paginate
from playwright.async_api import BrowserContext, async_playwright

import agentql

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# INPUT_CSV = os.path.join(SCRIPT_DIR, "input", "serps_test.csv")
INPUT_CSV = os.path.join(SCRIPT_DIR, "input", "job_search_pages.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "output", "job_data.csv")

# Create input/output directories if they don't exist
os.makedirs(os.path.join(SCRIPT_DIR, "input"), exist_ok=True)
os.makedirs(os.path.join(SCRIPT_DIR, "output"), exist_ok=True)

# Define the limit of the number of times to click the load more button or paginate
NUM_PAGES_TO_LOAD = 3

# Define the queries to interact with the page
LOAD_MORE_QUERY = """
{
    load_more_button(The button should include the words "load more", located near the bottom of the webpage.)
}
"""

COOKIES_QUERY = """
{
    cookies_form {
        accept_btn
        reject_btn
        close_btn
    }
}
"""

JOBS_QUERY = """
{
    total_number_of_jobs(integer)
    jobs[]
    {
        title(string)
        posting_url(string)
        company_name(string)
        full_time(boolean)
        department(string)
        location(string)
        start_date(string)
        deadline("DD/MM/YYYY")
        level(Allocate the job to one of the following levels: 'Apprenticeship', 'Graduate', 'Internship', 'MBA', 'APD', 'Other')
    }
}
"""

async def fetch_jobs(context: BrowserContext, url):
    """Open the given URL in a new tab and fetch the price of the product."""

    # Create a page in a new tab in the browser context and wrap it to get access to the AgentQL's querying API
    page = await agentql.wrap_async(context.new_page())
    await page.goto(url)
    await page.wait_for_page_ready_state()

    # Wait 5 seconds for the cookies popup to appear (some pages delay it to foil scripts such as these)
    await page.wait_for_timeout(5000)

    # SPECIAL CASE FOR MCKINSEY WEBSITE: MUST ACCEPT COOKIES TO LOAD MORE
    if "mckinsey" in url:
        # Accept cookies
        response = await page.query_elements(COOKIES_QUERY)
        if response.cookies_form.accept_btn is not None:
            await response.cookies_form.accept_btn.click()
            await page.wait_for_page_ready_state()
        elif response.cookies_form.close_btn is not None:
            await response.cookies_form.close_btn.click()
            await page.wait_for_page_ready_state()
    else:
        # Reject cookies or close popup
        response = await page.query_elements(COOKIES_QUERY)
        if response.cookies_form.reject_btn is not None:
            await response.cookies_form.reject_btn.click()
            await page.wait_for_page_ready_state()
        elif response.cookies_form.close_btn is not None:
            await response.cookies_form.close_btn.click()
            await page.wait_for_page_ready_state()

    # Attempt to load more jobs
    load_more_response = await page.query_elements(LOAD_MORE_QUERY)
    for _ in range(NUM_PAGES_TO_LOAD):
        if load_more_response.load_more_button:
            await load_more_response.load_more_button.click()
            await page.wait_for_page_ready_state()

    # Fetch job data
    print("Attempting pagination of", url)
    pages = await paginate(page, JOBS_QUERY, NUM_PAGES_TO_LOAD)
    print("Paginated", url)

    # Extract job listings from `pages`
    jobs = []
    for page_data in pages:
        jobs.extend(page_data.get("jobs", []))

    return jobs

async def get_jobs_across_websites():
    """Opens the given URLs in separate tabs in a single window and scrapes them for job listings."""
    # Check if input file exists
    if not os.path.exists(INPUT_CSV):
        print(f"Please create {INPUT_CSV} with a 'url' column containing the SERP URLs to scrape")
        return

    # Read URLs from CSV
    urls_df = pd.read_csv(INPUT_CSV)
    if 'url' not in urls_df.columns:
        print("Input CSV must contain a 'url' column")
        return

    all_jobs = []

    async with async_playwright() as playwright, await playwright.chromium.launch(headless=False) as browser, await browser.new_context() as context:
        # Fetch jobs concurrently for all platforms
        search_results = await asyncio.gather(*[fetch_jobs(context, url) for url in urls_df['url']])

        # Iterate through each result and collect jobs
        for jobs in search_results:
            all_jobs.extend(jobs)

    # Write all results to CSV
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Saved {len(all_jobs)} jobs to {OUTPUT_CSV}")
    else:
        print("No jobs found")

if __name__ == "__main__":
    asyncio.run(get_jobs_across_websites())
