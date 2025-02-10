"""Asynchronously scraptes job listings across websites in parallel."""

import os
import logging
import asyncio
from contextlib import asynccontextmanager
from playwright.async_api import BrowserContext, async_playwright
import agentql
import pandas as pd

os.environ["AGENTQL_API_KEY"] = "XO8uDp63d8UfhC41wkK1WuDrVoFQIV1xhQGtZgQ-OU5bX6OkUPjVDA"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear the log file by opening it in write mode
with open("log.txt", "w") as f:
    f.write("")  # This truncates the file

handler = logging.FileHandler("log.txt")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(SCRIPT_DIR, "input", "job_search_pages.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "output", "job_data.csv")

# Create input/output directories if they don't exist
os.makedirs(os.path.join(SCRIPT_DIR, "input"), exist_ok=True)
os.makedirs(os.path.join(SCRIPT_DIR, "output"), exist_ok=True)

# Define the number of attempts to paginate
PAGINATE_REATTEMPTS = 5

# Define the queries to interact with the page
LOAD_MORE_QUERY = """
{
    load_more_button(The button must include the words "load more", and is located near the bottom of the webpage.)
}
"""

MAX_PAGINATION_QUERY = """
{
    greatest_page_number(integer representing the maximum number of PAGES to paginate through, NOT the total number of results)
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
        job_title_text
        job_posting_url
        company_name
        job_type(full_time, part_time, internship, temporary, volunteer, or other)
        department
        location(string. The location of the main title job. Not the location of similar, related, or recommended jobs.)
        start_date
        deadline("DD/MM/YYYY")
        level(Allocate the job to one of the following levels: 'Apprenticeship', 'Graduate', 'Internship', 'MBA', 'APD', 'Other')
    }
}
"""


async def accept_cookies(page, url):
    """Accept cookies."""
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            async with record_exec_stats(f"Query cookies form for {url}"):
                response = await page.query_elements(COOKIES_QUERY, mode="standard")
            break
        except Exception as e:
            logger.error("Error querying cookies at %s: %s", url, str(e))
            continue
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            if response.cookies_form.accept_btn is not None:
                await response.cookies_form.accept_btn.click()
                async with record_exec_stats(
                    f"Wait for page ready after accepting cookies at {url}"
                ):
                    await page.wait_for_page_ready_state()
            elif response.cookies_form.close_btn is not None:
                await response.cookies_form.close_btn.click()
                async with record_exec_stats(
                    f"Wait for page ready after closing cookies at {url}"
                ):
                    await page.wait_for_page_ready_state()
            elif response.cookies_form.reject_btn is not None:
                await response.cookies_form.reject_btn.click()
                async with record_exec_stats(
                    f"Wait for page ready after rejecting cookies at {url}"
                ):
                    await page.wait_for_page_ready_state()
            break
        except Exception as e:
            logger.error("Error accepting cookies at %s: %s", url, str(e))
            continue


async def reject_cookies(page, url):
    """Reject cookies."""
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            response = await page.query_elements(COOKIES_QUERY, mode="standard")
            break
        except Exception as e:
            logger.error("Error querying cookies at %s: %s", url, str(e))
            continue
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            if response.cookies_form.reject_btn is not None:
                await response.cookies_form.reject_btn.click()
                async with record_exec_stats(
                    f"Wait for page ready after rejecting cookies at {url}"
                ):
                    await page.wait_for_page_ready_state()
            elif response.cookies_form.close_btn is not None:
                await response.cookies_form.close_btn.click()
                async with record_exec_stats(
                    f"Wait for page ready after closing cookies at {url}"
                ):
                    await page.wait_for_page_ready_state()
            elif response.cookies_form.accept_btn is not None:
                await response.cookies_form.accept_btn.click()
                async with record_exec_stats(
                    f"Wait for page ready after accepting cookies at {url}"
                ):
                    await page.wait_for_page_ready_state()
            break
        except Exception as e:
            logger.error("Error rejecting cookies at %s: %s", url, str(e))
            continue


async def load_more_jobs(load_more_response, page, url):
    """Click the load more button and wait for the page to load."""
    if load_more_response.load_more_button:
        for _ in range(PAGINATE_REATTEMPTS):
            try:
                await load_more_response.load_more_button.click()
                break
            except Exception as e:
                logger.error("Error clicking load more button at %s: %s", url, str(e))
                continue
        for _ in range(PAGINATE_REATTEMPTS):
            try:
                async with record_exec_stats(
                    f"Wait for page ready after load more at {url}"
                ):
                    await page.wait_for_page_ready_state()
                break
            except Exception as e:
                logger.error(
                    "Error waiting for page ready state at %s: %s", url, str(e)
                )
                continue
        return True
    return False


async def query_load_more_button(page, url):
    """Query the load more button."""
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            return await page.query_elements(LOAD_MORE_QUERY, mode="standard")
        except Exception as e:
            logger.error("Error querying load more button at %s: %s", url, str(e))
            continue
    return []


async def get_max_pagination_value(page, url, default_value):
    """Get the maximum pagination value."""
    try:
        async with record_exec_stats(f"Query max pagination value for {url}"):
            max_pagination_response = await page.query_data(MAX_PAGINATION_QUERY)
        max_pagination_value = max_pagination_response["greatest_page_number"]
        if max_pagination_value is None:
            max_pagination_value = default_value
        return max_pagination_value
    except Exception as e:
        logger.error("Error querying max pagination value at %s: %s", url, str(e))
        return default_value


async def paginate_to_next_page(page, url, current_page, final_page):
    """Paginate to the next page."""
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            async with record_exec_stats(
                f"Query pagination info for page {current_page} of {final_page} of {url}"
            ):
                pagination_info = await page.get_pagination_info()
            break
        except Exception as e:
            logger.error(
                "Error querying pagination info at page %d of %d at %s: %s",
                current_page,
                final_page,
                url,
                str(e),
            )
            continue
    if pagination_info.has_next_page:
        for _ in range(PAGINATE_REATTEMPTS):
            try:
                await pagination_info.navigate_to_next_page()
                break
            except Exception as e:
                logger.error(
                    "Error navigating to next page at page %d of %d at %s: %s",
                    current_page,
                    final_page,
                    url,
                    str(e),
                )
                continue
        for _ in range(PAGINATE_REATTEMPTS):
            try:
                async with record_exec_stats(
                    f"Wait for page ready after pagination to page {current_page} of {url}"
                ):
                    await page.wait_for_page_ready_state()
                break
            except Exception as e:
                logger.error(
                    "Error waiting for page ready state at page %d of %d at %s: %s",
                    current_page,
                    final_page,
                    url,
                    str(e),
                )
                continue


def is_valid_job_format(result, current_page, max_pagination_value, url):
    """Check if the syntax of a job listing is valid."""
    if "\n" in result:
        logger.error(
            "Potential hallucination at page %d of %d of %s. Invalid job format detected: %s.",
            current_page,
            max_pagination_value,
            url,
            result,
        )
        return False
    else:
        return True


async def fetch_jobs(context: BrowserContext, url):
    """Open the given URL in a new tab and fetch job listings."""

    # Create a page in a new tab in the browser context
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            page = await agentql.wrap_async(context.new_page())
            await page.goto(url)
            async with record_exec_stats(f"Wait for initial page ready at {url}"):
                await page.wait_for_page_ready_state()
            break
        except Exception as e:
            logger.error("Error opening %s: %s", url, str(e))
            continue

    # Wait 5 seconds for the cookies popup to appear
    for _ in range(PAGINATE_REATTEMPTS):
        try:
            await page.wait_for_timeout(5000)
            break
        except Exception as e:
            logger.error("Error waiting for 5 seconds at %s: %s", url, str(e))
            continue

    # SPECIAL CASE FOR MCKINSEY WEBSITE: MUST ACCEPT COOKIES TO LOAD MORE
    if "mckinsey" in url:
        await accept_cookies(page, url)
    else:
        await reject_cookies(page, url)

    pages = []
    result = []
    jobs = []
    identical_results_message = ""

    # Load more jobs section
    async with record_exec_stats(f"Query load more button for {url}"):
        load_more_response = await page.query_elements(LOAD_MORE_QUERY, mode="standard")

    if load_more_response.load_more_button:
        while load_more_response.load_more_button:
            async with record_exec_stats(f"Click load more button for {url}"):
                await load_more_jobs(load_more_response, page, url)
            async with record_exec_stats(f"Query load more button for {url}"):
                load_more_response = await page.query_elements(
                    LOAD_MORE_QUERY, mode="standard"
                )

        async with record_exec_stats(f"Query jobs data for page 1 of {url}"):
            result.append(await page.query_data(JOBS_QUERY, mode="standard"))
            pages += result
    else:
        # Pagination section
        current_page = 1
        final_page = 1
        prev_result = None

        async with record_exec_stats(f"Query max pagination value for page 1 of {url}"):
            final_page = await get_max_pagination_value(page, url, 1)

        while current_page <= final_page:
            async with record_exec_stats(
                f"Query max pagination for page {current_page} of {url}"
            ):
                final_page = await get_max_pagination_value(page, url, final_page)

            async with record_exec_stats(
                f"Query jobs data for page {current_page} of {url}"
            ):
                result = []
                result.append(await page.query_data(JOBS_QUERY, mode="standard"))

            # If the results are identical, pagination has failed to navigate to the next page
            if result == prev_result:
                logger.info(
                    "Identical results at pages %d and %d of %s",
                    current_page - 1,
                    current_page,
                    url,
                )
                break
            prev_result = result

            # If the results are valid, add them to the list of pages
            if is_valid_job_format(result, current_page, final_page, url):
                pages += result
            else:
                continue

            # If the current page is the last page, break the loop
            if current_page == final_page:
                break
            else:
                await paginate_to_next_page(page, url, current_page, final_page)
                current_page += 1

        # Fetch the job data for the last page
        result = []
        result.append(await page.query_data(JOBS_QUERY, mode="standard"))

    # Extract job listings from `pages`
    for page_data in pages:
        jobs.extend(page_data.get("jobs", []))

    logger.info("Found %d jobs on %s. %s", len(jobs), url, identical_results_message)
    print(f"Found {len(jobs)} jobs on {url}. {identical_results_message}")

    return jobs


@asynccontextmanager
async def record_exec_stats(operation_name="Operation"):
    """Async context manager to measure and log execution time of async operations.

    Args:
        operation_name: Name of the operation being measured (for logging)
    """
    loop = asyncio.get_running_loop()
    start_time = loop.time()

    try:
        yield
        end_time = loop.time()
        duration = end_time - start_time
        logger.info("%s completed in %.2f seconds", operation_name, duration)
    except Exception as e:
        end_time = loop.time()
        duration = end_time - start_time
        logger.error(
            "%s failed after %.2f seconds with error: %s",
            operation_name,
            duration,
            str(e),
        )
        raise


async def get_jobs_across_websites():
    """Opens the URLs in separate tabs in a single window and scrapes them."""
    # Check if input file exists
    if not os.path.exists(INPUT_CSV):
        logger.error(
            "Please create %s with a 'url' column containing the SERP URLs to scrape",
            INPUT_CSV,
        )
        return

    # Read URLs from CSV
    urls_df = pd.read_csv(INPUT_CSV)
    if "url" not in urls_df.columns:
        logger.error("Input CSV must contain a 'url' column")
        return

    all_jobs = []

    try:
        async with async_playwright() as playwright, await playwright.chromium.launch(
            headless=False
        ) as browser, await browser.new_context() as context, record_exec_stats(
            "Job scraping"
        ):
            # Fetch jobs concurrently for all platforms
            search_results = await asyncio.gather(
                *[fetch_jobs(context, url) for url in urls_df["url"]]
            )
    except Exception as e:
        logger.error("Error: %s", str(e))
        print(f"Error: {e}")
        return

    # Iterate through each result and collect jobs
    for jobs in search_results:
        all_jobs.extend(jobs)

    # Write all results to CSV
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info("Saved %d jobs to %s", len(all_jobs), OUTPUT_CSV)
    else:
        logger.warning("No jobs found")


if __name__ == "__main__":
    asyncio.run(get_jobs_across_websites())
