
// //  Not relevant jobs keywords list 
// const REJECT_KEYWORDS = ['intern', 'internship', 'stage', 'senior', 'sr.', 'staff', 'lead', 'principal', 'head of', 'manager', 'director']

// // Location keywords
// const LOCATION_KEYWORDS = ['amsterdam']

// // Company keywords
// const COMPANY_KEYWORDS = ['coolblue', 'jd.com']

// //Linkedin tag 
// const LINKEDIN_TAGS = ['viewed']

// function stage1Filter(jobData) {
//     const title = (jobData.title || '')
//     const location = (jobData.location || '').toLowerCase()
//     const company = (jobData.company || '').toLowerCase()
//     const linkedinTag = jobData.linkedinTag || []
   

//     const matchedKeyword = REJECT_KEYWORDS.find(keyword => {
//         const regex = new RegExp(`\\b${keyword}\\b`, 'i')
//         return regex.test(title)
//     })
//     if (matchedKeyword) {
//         return { status: 'not relevant', filterReason: `contains keyword: ${matchedKeyword}` }
//     }

//     if (!LOCATION_KEYWORDS.some(keyword => location.includes(keyword))) {
//         return { status: 'not relevant', filterReason: `location mismatch: ${location}` }
//     }

//     if (COMPANY_KEYWORDS.some(keyword => company.includes(keyword))) {
//         return { status: 'not relevant', filterReason: `excluded company: ${company}` }
//     }

//     if (LINKEDIN_TAGS.some(tag => linkedinTag.includes(tag))) {
//         return { status: 'not relevant', filterReason: 'already viewed' }
//     }
//     return { status: 'relevant', filterReason: null }
// }