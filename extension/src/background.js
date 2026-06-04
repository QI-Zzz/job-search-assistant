const API_BASE = 'http://localhost:8000'

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {

    if (message.type === 'SAVE_JOB') {
        fetch(`${API_BASE}/jobs/save`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(message.payload)
        })
        .then(response => {
            sendResponse({ success: true })
        })
        .catch(error => {
            console.error('Failed to save job:', error)
            sendResponse({ success: false, error: error.message })
        })
    }

    return true
})