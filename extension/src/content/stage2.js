
// // Languages required
// const languages = [    
//     'dutch', 
//     'nederlands',
//     'dutch language',
//     'dutch speaking',
//     'dutch proficiency',
//     'vereist nederlands',
//     'nederlandse taalvaardigheid',
//     "nederlandse taal",
//     "dutch fluency",
//     "dutch mandatory",
//     "nederlands vereist",
//     "nederlands mandatory"
// ]

// // Skills keywords
// const skill_keywords = ['sql', 'python', 'dbt', 'tableau', 'power bi', 'looker', 'bigquery', 'spark', 'airflow']

// // Help function
// checkExperience = (text) => {
//     const match = text.match(/(\d+)\+?\s*years/i)
//     if (match) {
//         const years = parseInt(match[1], 10)
//         return years < 3
//     }
//     return true
// }

// checkLanguages = (text) => {
//     return !languages.some(language => text.toLowerCase().includes(language))
// }

// checkSkills = (text) => {
//     // return skill_keywords.some(keyword => text.toLowerCase().includes(keyword))
//     const foundSkills = skill_keywords.filter(keyword => text.toLowerCase().includes(keyword))
//     return foundSkills
// }

// async function stage2Filter(jobData, descriptionEI) {
//     const description = descriptionEI.textContent || ''

//     const passExperience = checkExperience(description)
//     const passLanguages = checkLanguages(description)
//     const passSkills = checkSkills(description)
//     const card = document.querySelector(`.scaffold-layout__list-item[data-job-id="${jobData.jobID}"]`)

//     if (passExperience && passLanguages && passSkills.length>0) {
//         await JobStorage.setJobState(jobData.jobID, 'relevant', jobData.linkedinTag, null)
//         chrome.runtime.sendMessage({
//             type: "Post_stage2",
//             payload: {
//                 jobID: jobData.jobID,
//                 description: descriptionEI.textContent,
//                 status: 'relevant',
//                 collected_at: new Date().toISOString()
//             }
//         })
//         injectKeywordBadge(card, passSkills)
//         return { status: 'relevant', filterReason: null }
//     } else {
//         if (!passExperience) {
//             jobData.rejectReason = 'experience requirement'
//         } else if (!passLanguages) {
//             jobData.rejectReason = 'language requirement'
//         } else if (passSkills.length===0) {
//             jobData.rejectReason = 'skills requirement'
//         }       
//         await JobStorage.setJobState(jobData.jobID, 'not relevant', jobData.linkedinTag, jobData.rejectReason)
//         if (card) {
//             card.style.display = 'none'
//         }
//         chrome.runtime.sendMessage({
//             type: "Post_stage2",
//             payload: {
//                 jobID: jobData.jobID,
//                 description: descriptionEI.textContent,
//                 status: 'not relevant',
//                 filterReason: jobData.rejectReason,
//                 collected_at: new Date().toISOString()
//             }
//         })
//         if (!card) return  
//         injectRejectBadge(card, jobData.rejectReason)
//         return { status: 'not relevant', filterReason: jobData.rejectReason }
//     }
// }