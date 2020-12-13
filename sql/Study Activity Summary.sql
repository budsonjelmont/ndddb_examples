--Summarizes the study events that have occurred within the past X days.

PARAMETERS (DAYSAGO INTEGER DEFAULT 7)

SELECT fses.Cohort,
COUNT(Enrollment.SubjectID) AS "Total subjects (all time)",
COUNT(DISTINCT fses.id) AS "Total families (all time)",
COALESCE(SUM("Total time spent (hrs)"),0) AS "Calls made (hours)",
COALESCE(COUNT(consented.SubjectID),0) AS "Subjects consented",
COALESCE(COUNT(DISTINCT consented.familyid),0) AS "Families consented",
COALESCE(COUNT(mcr.SubjectID),0) AS "Chart reviews completed",
COALESCE(COUNT(dsmsfilled.SubjectID),0) AS "DSMs entered",
COALESCE(SUM(clingen.n_clingenreports),0) AS "Genetics reports entered (reports)",
COALESCE(COUNT(DISTINCT clingen.SubjectID),0) AS "Genetics reports entered (subjects)",
COALESCE(COUNT(seaver.SubjectID),0) AS "Seaver evaluations performed",
COALESCE(COUNT(pq.SubjectID),0) AS "Participant questionnaires completed",
COALESCE(SUM(srs2.n_srs2ps_complete)+SUM(srs2.n_srs2sa_complete)+SUM(srs2.n_srs2adult_complete),0) AS "SRS-2's completed (questionnaires)",
COALESCE(COUNT(seqreviews.id),0) AS "Seq submission reviews completed",
COALESCE(COUNT(marrreviews.id),0) AS "Microarray submission reviews completed",
COALESCE(SUM(MApreppedsamples.n_prepared),0) AS "Samples prepared for microarray",
COALESCE(COUNT(DISTINCT MApreppedsamples.familyid),0) AS "Families prepared for microarray",
COALESCE(SUM(MAsubbedsamples.n_submitted),0) AS "Samples submitted for microarray",
COALESCE(COUNT(DISTINCT MAsubbedsamples.familyid),0) AS "Families submitted for microarray",
COALESCE(SUM(seqpreppedsamples.n_prepared),0) AS "Samples prepared for sequencing",
COALESCE(COUNT(DISTINCT seqpreppedsamples.familyid),0) AS "Families prepared for sequencing",
COALESCE(SUM(seqsubbedsamples.n_submitted),0) AS "Samples submitted for sequencing",
COALESCE(COUNT(DISTINCT seqsubbedsamples.familyid),0) AS "Families submitted for sequencing",
COALESCE(COUNT(resreturn.id),0) AS "Results returned to providers"
FROM Project.study.Enrollment
JOIN Project.lists.FamilySampleEnrollmentSummary AS fses
ON fses.id = Project.study.Enrollment.Datasets.Demographics.familyid
--Contact Attempts
LEFT JOIN (
   SELECT "Contact Attempts".Datasets."Progress Notes".f_idnum,
   SUM(attempt_time_calc_v2)/60 AS "Total time spent (hrs)"
   FROM Project."Enrollment Portal".study."Contact Attempts"
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY',date, CURDATE()) <= DAYSAGO
   GROUP BY "Contact Attempts".Datasets."Progress Notes".f_idnum
) AS callsmade
ON callsmade.f_idnum = Enrollment.f_idnum
--Consented
LEFT JOIN (
   SELECT Enrollment.SubjectID,
   Enrollment.Datasets.Demographics.familyid
   FROM Project.study.Enrollment
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY',GREATEST(consentdate_gco151766,consentdate_gco121490), CURDATE()) <= DAYSAGO
) AS consented
ON consented.SubjectID = Enrollment.SubjectID
--Sequencing reviews performed
LEFT JOIN (
   SELECT id
   FROM Project.lists.Families
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY', GREATEST(seqsub_epi25_reviewdate, seqsub_asdepi_reviewdate), CURDATE()) <= DAYSAGO
   GROUP BY id
) AS seqreviews
ON CAST(seqreviews.id AS INTEGER) = Project.study.Enrollment.Datasets.Demographics.familyid
--Microarray reviews performed
LEFT JOIN (
   SELECT id
   FROM Project.lists.Families
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY', marrsub_asdepi_reviewdate, CURDATE()) <= DAYSAGO
   GROUP BY id
) AS marrreviews
ON CAST(marrreviews.id AS INTEGER) = Project.study.Enrollment.Datasets.Demographics.familyid
--Charts reviewed
LEFT JOIN (
   SELECT SubjectID
   FROM Project.study."Manual Chart Review" AS mcr
   WHERE mcr.chart_review_complete = 2
   AND TIMESTAMPDIFF('SQL_TSI_DAY',date, CURDATE()) <= DAYSAGO
) AS mcr
ON mcr.SubjectID = Enrollment.SubjectID
--DSMs filled
LEFT JOIN (
   SELECT SubjectID
   FROM Project.study.DSM5 AS dsm
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY',date, CURDATE()) <= DAYSAGO
) AS dsmsfilled
ON dsmsfilled.SubjectID = Enrollment.SubjectID
--Clinical genetics reports entered
LEFT JOIN (
   SELECT SubjectID,
   COUNT(SubjectID) AS "n_clingenreports"
   FROM Project.study."Clinical Genetics Report" AS clingen
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY',date, CURDATE()) <= DAYSAGO
   GROUP BY SubjectID
) AS clingen
ON clingen.SubjectID = Enrollment.SubjectID
--Seaver evaluations performed
LEFT JOIN (
   SELECT SubjectID
   FROM Project.study."Seaver Assessment" AS seaver
   WHERE TIMESTAMPDIFF('SQL_TSI_DAY',date, CURDATE()) <= DAYSAGO
) AS seaver
ON seaver.SubjectID = Enrollment.SubjectID
--Participant Questionnaires completed
LEFT JOIN (
   SELECT SubjectID
   FROM Project.study."Participant Questionnaire" AS pq
   WHERE pq.participant_questionnaire_complete = 2
   AND TIMESTAMPDIFF('SQL_TSI_DAY',date, CURDATE()) <= DAYSAGO
) AS pq
ON pq.SubjectID = Enrollment.SubjectID
--SRS2 completed
LEFT JOIN (
   SELECT Enrollment.SubjectID,
   COUNT(srs2ps.SubjectID) AS "n_srs2ps_complete",
   COUNT(srs2sa.SubjectID) AS "n_srs2sa_complete",
   COUNT(srs2adult.SubjectID) AS "n_srs2adult_complete"
   FROM Project.study.Enrollment
   LEFT JOIN Project.study."SRS2 (Preschool)" AS srs2ps
   ON srs2ps.SubjectID = Enrollment.SubjectID
   LEFT JOIN Project.study."SRS2 (School Age)" AS srs2sa
   ON srs2sa.SubjectID = Enrollment.SubjectID
   LEFT JOIN Project.study."SRS2 (Adult)" AS srs2adult
   ON srs2adult.SubjectID = Enrollment.SubjectID
   WHERE (srs2_ps_complete = 2 OR srs2_sa_complete = 2 OR srs2_adultsr_complete = 2)
   AND (TIMESTAMPDIFF('SQL_TSI_DAY',srs2ps.date, CURDATE()) <= DAYSAGO OR TIMESTAMPDIFF('SQL_TSI_DAY',srs2sa.date, CURDATE()) <= DAYSAGO OR TIMESTAMPDIFF('SQL_TSI_DAY',srs2adult.date, CURDATE()) <= DAYSAGO)
   GROUP BY Enrollment.SubjectID
) AS srs2
ON srs2.SubjectID = Enrollment.SubjectID
--Samples prepared for microarray
LEFT JOIN (
 SELECT Demographics.SubjectID,
 Demographics.familyid,
 COUNT(Biospecimens.Name) AS "n_prepared",
 FROM Project.study.Demographics
 JOIN Project."Genetics Portal".samples.Biospecimens
 ON Biospecimens."Participant Id" = Demographics.SubjectID
 JOIN Project."Genetics Portal".assay.General.Sequencing.Data
 ON Data.SampleID= Biospecimens.Name
 WHERE Data.Run.SequencingType.SequencingType='Infinium GSA Array'
 AND TIMESTAMPDIFF('SQL_TSI_DAY',Data.Run.preparedate, CURDATE()) <= DAYSAGO
 GROUP BY Demographics.SubjectID, Demographics.familyid
) AS MApreppedsamples
ON MApreppedsamples.SubjectID = Enrollment.SubjectID
--Samples submitted for microarray
LEFT JOIN (
 SELECT Demographics.SubjectID,
 Demographics.familyid,
 COUNT(Biospecimens.Name) AS "n_submitted",
 FROM Project.study.Demographics
 JOIN Project."Genetics Portal".samples.Biospecimens
 ON Biospecimens."Participant Id" = Demographics.SubjectID
 JOIN Project."Genetics Portal".assay.General.Sequencing.Data
 ON Data.SampleID= Biospecimens.Name
 WHERE Data.Run.SequencingType.SequencingType='Infinium GSA Array'
 AND TIMESTAMPDIFF('SQL_TSI_DAY',Data.Run.submitdate, CURDATE()) <= DAYSAGO
 GROUP BY Demographics.SubjectID, Demographics.familyid
) AS MAsubbedsamples
ON MAsubbedsamples.SubjectID = Enrollment.SubjectID
--Samples prepared for sequencing
LEFT JOIN (
 SELECT  Demographics.SubjectID,
 Demographics.familyid,
 COUNT(Biospecimens.Name) AS "n_prepared",
 FROM Project.study.Demographics
 JOIN Project."Genetics Portal".samples.Biospecimens
 ON Biospecimens."Participant Id" = Demographics.SubjectID
 JOIN Project."Genetics Portal".assay.General.Sequencing.Data
 ON Data.SampleID= Biospecimens.Name
 WHERE Data.Run.SequencingType.SequencingType IN ('WES','WES+','Sanger')
 AND TIMESTAMPDIFF('SQL_TSI_DAY',Data.Run.preparedate, CURDATE()) <= DAYSAGO
 GROUP BY Demographics.SubjectID, Demographics.familyid
) AS seqpreppedsamples
ON seqpreppedsamples.SubjectID = Enrollment.SubjectID
--Samples submitted for sequencing
LEFT JOIN (
 SELECT Demographics.SubjectID,
 Demographics.familyid,
 COUNT(Biospecimens.Name) AS "n_submitted",
 FROM Project.study.Demographics
 JOIN Project."Genetics Portal".samples.Biospecimens
 ON Biospecimens."Participant Id" = Demographics.SubjectID
 JOIN Project."Genetics Portal".assay.General.Sequencing.Data
 ON Data.SampleID= Biospecimens.Name
 WHERE Data.Run.SequencingType.SequencingType IN ('WES','WES+','Sanger')
 AND TIMESTAMPDIFF('SQL_TSI_DAY',Data.Run.submitdate, CURDATE()) <= DAYSAGO
 GROUP BY Demographics.SubjectID, Demographics.familyid
) AS seqsubbedsamples
ON seqsubbedsamples.SubjectID = Enrollment.SubjectID
--Results returned
LEFT JOIN (
   SELECT id
   FROM Project.lists.Families
   WHERE seqsub_resreturn IS TRUE
   AND TIMESTAMPDIFF('SQL_TSI_DAY', seqsub_resreturn_date, CURDATE()) <= DAYSAGO
   GROUP BY id
) AS resreturn
ON CAST(resreturn.id AS INTEGER) = Project.study.Enrollment.Datasets.Demographics.familyid
GROUP BY Cohort
