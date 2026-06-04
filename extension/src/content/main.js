let lastProcessedJobID = null

function getJobIDFromURL() {
    const match = window.location.href.match(/currentJobId=(\d+)/)
    return match ? match[1] : null
}

function extractJobData(pane, jobID) {
    return {
        jobID,
        title: pane.querySelector('h1')?.textContent.trim() || null,
        company: pane.querySelector(
            '.job-details-jobs-unified-top-card__company-name'
        )?.textContent.trim() || null,
        location: pane.querySelector(
            '.job-details-jobs-unified-top-card__primary-description-container'
        )?.textContent.trim() || null,
        description: pane.querySelector('.mt4')?.textContent.trim() || null,
        linkedin_url: window.location.href,
        source: 'linkedin',
        collected_at: new Date().toISOString()
    }
}

async function processDetailPane(pane) {
    const jobID = getJobIDFromURL()
    if (!jobID) return
    if (jobID === lastProcessedJobID) return

    const h1 = pane.querySelector('h1')
    if (!h1) return

    lastProcessedJobID = jobID

    // Check if already saved
    const seen = await JobStorage.hasBeenSeen(jobID)
    if (seen) {
        markAlreadySaved(pane)
        return
    }

    // Extract and inject save button
    const jobData = extractJobData(pane, jobID)

    injectSaveButton(pane, jobData, async () => {
        // Save to local storage
        await JobStorage.setJobState(jobID, 'saved', null, null)

        // POST to FastAPI
        chrome.runtime.sendMessage({
            type: 'SAVE_JOB',
            payload: jobData
        })
    })
}

function init() {
    const observer = new MutationObserver(() => {
        const detailPane = document.querySelector(
            '.jobs-search__job-details--wrapper'
        )
        if (!detailPane) return
        processDetailPane(detailPane)
    })

    observer.observe(document.body, {
        childList: true,
        subtree: true
    })
}

init()