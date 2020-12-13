--Assign a status to each family reflecting which members have a sample in the collection.
--Trio: Proband, Mom, and Dad have samples
--Quartet: Proband, Mom, Dad, and >=1 Sibling have samples
--Proband and mother (father not available): Proband and Mom have samples, Dad is not available
--Proband and father (mother not available): Proband and Dad have samples, Mom is not available
--Proband and mother: Proband and Mom have samples
--Proband and father: Proband and Dad have samples
--Proband only (parents not available): Proband has samples, Mom and Dad are not available
--Proband only: Proband has samples
--Proband missing: Proband does not have a sample
--Other: Default if no other category applies

SELECT DISTINCT Families.id,Families.fnum,
CASE WHEN ((COUNT(probandSamples.familyid) > 0 AND COUNT(momSamples.familyid) > 0) AND (COUNT(dadSamples.familyid) > 0 AND COUNT(sibSamples.familyid) > 0)) THEN('Quartet') ELSE(
	CASE WHEN ((COUNT(probandSamples.familyid) > 0 AND COUNT(momSamples.familyid) > 0) AND COUNT(dadSamples.familyid) > 0) THEN('Trio') ELSE(
		CASE WHEN ((COUNT(probandSamples.familyid) > 0 AND COUNT(momSamples.familyid) > 0) AND (COUNT(dadSamples.familyid) = 0 AND 		COUNT(dadNotAvail.familyid) > 0)) THEN('Proband and mother (father not available)') ELSE(
	    	CASE WHEN ((COUNT(probandSamples.familyid) > 0 AND COUNT(dadSamples.familyid) > 0) AND (COUNT(momSamples.familyid) = 0 			AND COUNT(momNotAvail.familyid) > 0)) THEN('Proband and father (mother not available)') ELSE(
        		CASE WHEN ((COUNT(probandSamples.familyid) > 0 AND COUNT(momSamples.familyid) > 0) AND (
            	COUNT(dadSamples.familyid) = 0)) THEN('Proband and mother') ELSE(
            		CASE WHEN ((COUNT(probandSamples.familyid) > 0 AND COUNT(dadSamples.familyid) > 0) AND (
            		COUNT(momSamples.familyid) = 0)) THEN('Proband and father') ELSE(
                		CASE WHEN (COUNT(probandSamples.familyid) = 0) THEN('Proband missing') ELSE(
                    		CASE WHEN (COUNT(probandSamples.familyid) > 0 AND (COUNT(momNotAvail.familyid) > 0 AND											COUNT(dadNotAvail.familyid) > 0)) THEN('Proband only (parents not available') ELSE(
                        		CASE WHEN (COUNT(probandSamples.familyid) > 0) THEN('Proband only') ELSE('Other')
   								END)
	   						END)
   						END)
   					END)
   				END)
	   		END)
   	END)
END)
END AS "FamStatus",
famSamples.HasSample,
famNotAvail.NotAvail
FROM Project.lists.Families
-- All family members w/ a sample
LEFT JOIN (SELECT Demographics.familyid, GROUP_CONCAT(DISTINCT demo_relation, ',') AS "HasSample" from Project.study.Demographics
            JOIN Project."Genetics Portal".samples.Biospecimens
			ON Demographics.SubjectID = Biospecimens."Participant Id"
            GROUP BY familyid
          ) AS famSamples
ON Families.id = famSamples.familyid
-- Probands w/ samples
LEFT JOIN (SELECT Demographics.familyid, Biospecimens."Participant Id" from Project.study.Demographics
            JOIN Project."Genetics Portal".samples.Biospecimens
			ON Demographics.SubjectID = Biospecimens."Participant Id"
			WHERE lcase(Demographics.demo_relation) = 'proband'
          ) AS probandSamples
ON Families.id = probandSamples.familyid
-- Mothers w/ samples
LEFT JOIN (SELECT Demographics.familyid, Biospecimens."Participant Id" from Project.study.Demographics
            JOIN Project."Genetics Portal".samples.Biospecimens
			ON Demographics.SubjectID = Biospecimens."Participant Id"
			WHERE lcase(Demographics.demo_relation) = 'mother'
          ) AS momSamples
ON Families.id = momSamples.familyid
-- Fathers w/ samples
LEFT JOIN (SELECT Demographics.familyid, Biospecimens."Participant Id" from Project.study.Demographics
            JOIN Project."Genetics Portal".samples.Biospecimens
			ON Demographics.SubjectID = Biospecimens."Participant Id"
			WHERE lcase(Demographics.demo_relation) = 'father'
          ) AS dadSamples
ON Families.id = dadSamples.familyid
-- Siblings w/ samples
LEFT JOIN (SELECT Demographics.familyid, Biospecimens."Participant Id" from Project.study.Demographics
            JOIN Project."Genetics Portal".samples.Biospecimens
			ON Demographics.SubjectID = Biospecimens."Participant Id"
			WHERE lcase(Demographics.demo_relation) = 'sibling'
          ) AS sibSamples
ON Families.id = sibSamples.familyid
-- Family members not available
LEFT JOIN (SELECT Demographics.familyid, GROUP_CONCAT(DISTINCT demo_relation, ',') AS "NotAvail" from Project.study.Demographics
			WHERE (demo_dead = true OR demo_nocontact = true)
            GROUP BY familyid
          ) AS famNotAvail
ON Families.id = famNotAvail.familyid
-- Mothers not available
LEFT JOIN (SELECT Demographics.familyid from Project.study.Demographics
			WHERE (demo_dead = true OR demo_nocontact = true)
            AND lcase(Demographics.demo_relation) = 'mother'
            GROUP BY familyid
          ) AS momNotAvail
ON Families.id = momNotAvail.familyid
-- Fathers not available
LEFT JOIN (SELECT Demographics.familyid from Project.study.Demographics
			WHERE (demo_dead = true OR demo_nocontact = true)
            AND lcase(Demographics.demo_relation) = 'father'
            GROUP BY familyid
          ) AS dadNotAvail
ON Families.id = dadNotAvail.familyid
GROUP BY Families.id,Families.fnum,HasSample,NotAvail
