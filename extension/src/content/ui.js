function injectSaveButton(pane, jobData, onSave) {
    // Remove existing button if present
    const existing = document.getElementById('jsa-save-btn')
    if (existing) existing.remove()

    // Find injection point - below h1
    const title = pane.querySelector('h1')
    if (!title) return

    // Create wrapper
    const wrapper = document.createElement('div')
    wrapper.style.marginTop = '8px'
    wrapper.style.marginBottom = '8px'

    // Create button
    const btn = document.createElement('button')
    btn.id = 'jsa-save-btn'
    btn.textContent = 'Save Job'
    btn.style.padding = '8px 16px'
    btn.style.backgroundColor = '#0a66c2'
    btn.style.color = 'white'
    btn.style.border = 'none'
    btn.style.borderRadius = '16px'
    btn.style.cursor = 'pointer'
    btn.style.fontSize = '14px'
    btn.style.fontWeight = '600'

    btn.addEventListener('click', async () => {
        btn.textContent = 'Saving...'
        btn.disabled = true
        await onSave()
        btn.textContent = '✓ Saved'
        btn.style.backgroundColor = '#057642'
    })

    wrapper.appendChild(btn)
    title.insertAdjacentElement('afterend', wrapper)
}

function markAlreadySaved(pane) {
    const existing = document.getElementById('jsa-save-btn')
    if (existing) existing.remove()

    const title = pane.querySelector('h1')
    if (!title) return

    const badge = document.createElement('div')
    badge.id = 'jsa-save-btn'
    badge.textContent = '✓ Already Saved'
    badge.style.marginTop = '8px'
    badge.style.color = '#057642'
    badge.style.fontSize = '14px'
    badge.style.fontWeight = '600'

    title.insertAdjacentElement('afterend', badge)
}