const STORE_KEY = 'job_saver_data'

const JobStorage = {

    getAllJobsStates: async function() {
        const result = await chrome.storage.local.get([STORE_KEY])
        return result[STORE_KEY] || {}
    },

    setJobState: async function(jobID, status = null, linkedinTag = null, filterReason = null) {
        const current = await this.getAllJobsStates()
        const savedAt = new Date().toISOString()
        current[jobID] = { status, linkedinTag, savedAt, filterReason }
        await chrome.storage.local.set({ [STORE_KEY]: current })
    },

    getJobState: async function(jobID) {
        const current = await this.getAllJobsStates()
        return current[jobID] || null
    },

    hasBeenSeen: async function(jobID) {
        const state = await this.getJobState(jobID)
        return !!state
    },

    clearAll: async function() {
        await chrome.storage.local.remove([STORE_KEY])
    }
}