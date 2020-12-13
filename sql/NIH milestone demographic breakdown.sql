-- For all ASD-Epi families stratified by collection status, show the # of subjects reporting in each Race/Ethnicity category. Count only collected individuals, and count only 1 sibling if Status='Quartet'. Do not count siblings/other family members for any other status.

SELECT AllFamsStatus.FamStatus,
Demo.demo_ethnic,
--Asian	African American/Black	Caucasian/White	Mixed	Native American/Alaskan Native (First Peoples)	Other	Unknown
SUM(CASE WHEN Demo.demo_race = 'Asian' THEN 1 ELSE 0 END) AS "Asian",
SUM(CASE WHEN Demo.demo_race = 'African American/Black' THEN 1 ELSE 0 END) AS "African American/Black",
SUM(CASE WHEN Demo.demo_race = 'Caucasian/White' THEN 1 ELSE 0 END) AS "Caucasian/White",
SUM(CASE WHEN Demo.demo_race = 'Mixed' THEN 1 ELSE 0 END) AS "Mixed",
SUM(CASE WHEN Demo.demo_race = 'Native American/Alaskan Native' THEN 1 ELSE 0 END) AS "Native American/Alaskan Native",
SUM(CASE WHEN Demo.demo_race = 'Other' THEN 1 ELSE 0 END) AS "Other",
SUM(CASE WHEN Demo.demo_race = 'Unknown' THEN 1 ELSE 0 END) AS "Unknown",
SUM(CASE WHEN Demo.demo_race IS NULL THEN 1 ELSE 0 END) AS "Not reported",
COUNT(demo_race) AS "Total (ethnicity)",
FROM Project."Enrollment Portal".lists."AllFamsStatus (collected)" AS AllFamsStatus
JOIN Project.lists.FamilySampleEnrollmentSummary AS famenrollov
ON famenrollov.id = AllFamsStatus.id
JOIN Project.study."Collected trio fam members + single sibling per family" AS Demo
ON AllFamsStatus.id = Demo.familyid
WHERE famenrollov.cohort = 'ASD,Epi'
AND (
   (AllFamsStatus.FamStatus = 'Trio' AND demo_relation IN ('Proband','Mother','Father'))
	OR (AllFamsStatus.FamStatus = 'Quartet' AND demo_relation IN ('Proband','Mother','Father','Sibling'))
	OR (AllFamsStatus.FamStatus IN ('Proband and mother (father not available)','Proband and mother') AND Demo.demo_relation IN ('Proband','Mother'))
	OR (AllFamsStatus.FamStatus IN ('Proband and father (mother not available)','Proband and father') AND Demo.demo_relation IN ('Proband','Father'))
	OR (AllFamsStatus.FamStatus IN ('Proband only','Proband only (parents not available)') AND Demo.demo_relation='Proband')
)
GROUP BY AllFamsStatus.FamStatus, Demo.demo_ethnic
PIVOT "Asian","African American/Black","Caucasian/White","Mixed","Native American/Alaskan Native","Other","Unknown","Not reported","Total (ethnicity)" BY demo_ethnic IN ('Hispanic','Not Hispanic','Unknown',NULL)
