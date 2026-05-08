#!/usr/bin/env python3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import csv, time, re, os
from datetime import datetime, timedelta

TODAY     = datetime.today()
YESTERDAY = TODAY - timedelta(days=1)
START_DATE = YESTERDAY.strftime("%m/%d/%Y")
END_DATE   = TODAY.strftime("%m/%d/%Y")
SEARCH_URL = "https://search.jeffersondeeds.com/insttype.php"
OUTPUT_CSV = os.path.expanduser(f"~/Downloads/lis_pendens_{TODAY.strftime('%Y%m%d')}.csv")

def log(msg, level=""):
    icon = {"OK":"✅","ERR":"❌","WARN":"⚠️ ","STEP":"▶️ "}.get(level,"   ")
    print(f"{datetime.now().strftime('%H:%M:%S')}  {icon}  {msg}")

def extract_address(text):
    patterns = [
        r'\d{2,5}\s+[A-Za-z0-9][A-Za-z0-9\s]+(?:Street|Avenue|Road|Drive|Boulevard|Lane|Court|Way|Place|Circle|Parkway|Pike|Blvd|Ave|Rd|Dr|Ln|Ct|St|Pl|Cir|Pkwy)[,\s]+(?:Louisville|Jeffersontown|Shively|Middletown|Anchorage|Fern Creek|Okolona)[,\s]+KY[\s]+\d{5}',
        r'\d{2,5}\s+[A-Za-z][A-Za-z0-9\s]{3,35}(?:Blvd|Ave|Rd|Dr|Ln|Ct|St|Pl|Cir|Way|Pike)[,.\s]+(?:Louisville|KY)',
        r'(?:Common Address|Property Address|Address)[:\s]+([^\n\r]{10,80})',
    ]
    for p in patterns:
        hits = re.findall(p, text, re.IGNORECASE)
        if hits:
            addr = hits[0].strip().rstrip(".,")
            if len(addr) > 10:
                return addr
    return None

def run():
    print()
    print("="*60)
    print("  Jefferson County KY - Lis Pendens Scraper")
    print(f"  Range: {START_DATE}  to  {END_DATE}")
    print("="*60)
    print()

    opts = Options()
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    log("Starting Chrome...", "STEP")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    wait = WebDriverWait(driver, 20)
    records = []

    try:
        log("Loading search page...", "STEP")
        driver.get(SEARCH_URL)
        time.sleep(3)
        log("Page loaded", "OK")

        log("Finding dropdowns...", "STEP")
        selects = driver.find_elements(By.TAG_NAME, "select")
        log(f"Found {len(selects)} dropdown(s)", "OK")

        selected = False
        for sel in selects:
            try:
                s = Select(sel)
                options = [o.text.strip() for o in s.options]
                if any("LIS PENDENS" in o for o in options):
                    s.select_by_visible_text("LIS PENDENS")
                    log("Selected LIS PENDENS", "OK")
                    selected = True
                    break
            except Exception as e:
                log(f"Dropdown error: {e}", "WARN")

        if not selected:
            raise Exception("Could not find LIS PENDENS dropdown")

        time.sleep(1)

        log("Finding date fields...", "STEP")
        all_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text'], input:not([type='submit']):not([type='button']):not([type='image'])")
        date_fields = []
        for inp in all_inputs:
            name = (inp.get_attribute("name") or "").lower()
            if "date" in name or "start" in name or "end" in name:
                date_fields.append(inp)

        if len(date_fields) >= 2:
            date_fields[0].clear()
            date_fields[0].send_keys(START_DATE)
            log(f"Start date: {START_DATE}", "OK")
            date_fields[1].clear()
            date_fields[1].send_keys(END_DATE)
            log(f"End date: {END_DATE}", "OK")
        else:
            text_inputs = [i for i in all_inputs if i.is_displayed()]
            if len(text_inputs) >= 2:
                text_inputs[-2].clear()
                text_inputs[-2].send_keys(START_DATE)
                text_inputs[-1].clear()
                text_inputs[-1].send_keys(END_DATE)
                log("Set dates via fallback", "WARN")

        time.sleep(1)

        log("Submitting search...", "STEP")
        submitted = False
        for val in ["Execute Search", "Search", "Submit"]:
            try:
                btn = driver.find_element(By.XPATH, f"//input[@value='{val}']")
                btn.click()
                log(f"Clicked: {val}", "OK")
                submitted = True
                break
            except Exception:
                pass

        if not submitted:
            for btn in driver.find_elements(By.CSS_SELECTOR, "input[type='submit']"):
                try:
                    btn.click()
                    submitted = True
                    break
                except Exception:
                    pass

        if not submitted:
            raise Exception("Could not click submit")

        time.sleep(4)
        body_text = driver.find_element(By.TAG_NAME, "body").text
        log(f"Results loaded. Preview: {body_text[:200]}", "OK")

        view_links = driver.find_elements(By.LINK_TEXT, "VIEW")
        if not view_links:
            view_links = driver.find_elements(By.PARTIAL_LINK_TEXT, "VIEW")

        total = len(view_links)
        log(f"Found {total} filing(s)", "OK")

        if total == 0:
            log(f"Page text: {body_text[:500]}", "WARN")
            return

        hrefs = [l.get_attribute("href") for l in view_links]

        rows_data = []
        for row in driver.find_elements(By.CSS_SELECTOR, "table tr"):
            cells = row.find_elements(By.TAG_NAME, "td")
            if cells:
                rows_data.append([c.text.strip() for c in cells])

        main_window = driver.current_window_handle

        for i, href in enumerate(hrefs):
            log(f"Filing {i+1}/{total}", "STEP")
            rec = {"date":"","defendants":"","address":"","pdf_link":href,"notes":[]}

            if i < len(rows_data):
                for cell in rows_data[i]:
                    dm = re.search(r'\d{2}/\d{2}/\d{4}', cell)
                    if dm and not rec["date"]:
                        rec["date"] = dm.group()
                    elif len(cell) > 5 and cell not in ("VIEW","DETAILS","") and not rec["defendants"]:
                        rec["defendants"] = cell

            doc_text = ""
            for attempt in range(1, 4):
                try:
                    driver.execute_script(f"window.open('{href}','_blank');")
                    time.sleep(3)
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(2)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    doc_text = driver.find_element(By.TAG_NAME, "body").text
                    if doc_text:
                        log(f"  Doc loaded ({len(doc_text)} chars)", "OK")
                        break
                except Exception as e:
                    log(f"  Attempt {attempt} failed: {e}", "WARN")
                    time.sleep(2)

            if doc_text:
                if not rec["date"]:
                    dm = re.search(r'\d{2}/\d{2}/\d{4}', doc_text)
                    if dm:
                        rec["date"] = dm.group()
                addr = extract_address(doc_text)
                if addr:
                    rec["address"] = addr
                    log(f"  Address: {addr}", "OK")
                else:
                    rec["address"] = "Open PDF manually"
                    rec["notes"].append("Address not extracted")
                    log("  Address not found", "WARN")
            else:
                rec["address"] = "Document failed to load"
                rec["notes"].append("Load failed x3")
                log("  Document failed", "ERR")

            driver.close()
            driver.switch_to.window(main_window)
            time.sleep(1)
            rec["notes"] = " | ".join(rec["notes"])
            records.append(rec)

    except Exception as e:
        log(f"Fatal error: {e}", "ERR")
    finally:
        driver.quit()

    if records:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Date","Defendants/Parties","Property Address","PDF Link","Notes"])
            for r in records:
                w.writerow([r["date"],r["defendants"],r["address"],r["pdf_link"],r["notes"]])
        print()
        print("="*60)
        print(f"  DONE - {len(records)} filings saved to:")
        print(f"  {OUTPUT_CSV}")
        print("="*60)
    else:
        print("No records saved.")

if __name__ == "__main__":
    run()

