// Wait the page to be ready, then init it
function init() {

    const jobListContainer = document.querySelector('.scaffold-layout__list')
    
    if (!jobListContainer) {
        return
    }
    
    const callback = (mutations) => {
        mutations.forEach(mutation => {
            mutation.addedNodes.forEach(node => {
                if (node.nodeType !== Node.ELEMENT_NODE) return
                if (!node.matches('.job-card-container')) return
                processJobCard(node)
                
            })
        })
    }
    const observer = new MutationObserver(callback)

    observer.observe(jobListContainer, { childList: true, subtree: true })


}

// Process a single job card
async function processJobCard(card) {
    const jdInfo = card.querySelector('.job-card-container__link')
    if (!jdInfo) return
    
    const jobID = card.getAttribute('data-job-id') || null
    if (!jobID) return

    const seen = await JobStorage.hasBeenSeen(jobID)
    if (seen) return

    const footerTags = Array.from(
        card.querySelectorAll('.job-card-container__footer-wrapper li')
    )
        .map(el => el.textContent.trim())
        .filter(Boolean)

    const jobData ={
        jobID: jobID,
        title: jdInfo.textContent.trim() || null,
        company: card.querySelector('.artdeco-entity-lockup__subtitle span')?.textContent.trim() || null,
        location: card.querySelector('.artdeco-entity-lockup__caption span')?.textContent.trim() || null,
        linkedinTag: footerTags
    }

    const result =stage1Filter(jobData)

    if (result === 'reject') {
        card.style.display = 'none'
        await JobStorage.setJobState(jobID, 'not relevant', jobData.linkedinTag)
    } else {
        onDetailPaneLoaded(jobData)
    }
}

// Handle detail pane
function onDetailPaneLoaded(jobData) {
    const detailPane = document.querySelector('.job-search__job-details--wrapper')
    if (!detailPane) return
    
    const observer = new MutationObserver(mutations => {
        mutations.forEach(mutation => {
            mutation.addedNodes.forEach(node => {
                const descriptionE1 = node.matches('.jobs-details__content') 
                    ? node 
                    : node.querySelector('.jobs-details__content')
                if (!descriptionE1) return

                stage2Filter(jobData, descriptionE1)
                observer.disconnect()
            })
        })
    })

    observer.observe(detailPane, { childList: true, subtree: true })

}


// start everything
init()