-- Report of all samples w/ at least one QC event recorded (NanoDrop, agarose gel, or PicoGreen quant). For each sample, show the values recorded for its most recent QC event of each type
SELECT
BS.Name AS "Sample ID",
Enrollment.f_idnum,
(MostRecentCleanup.SampleID IS NOT NULL) AS "Clean-up performed?",
MostRecentCleanup.PostCleanVol,
nReads,
AvgDNA,
Avg260280,
Avg260230,
StdDNA,
Std260280,
Std260230,
MedDNA,
Med260280,
Med260230,
BandVis,
DNAdegr,
ProtContam,
RNAcontam,
PassesGelQC,
Conc AS "PicoGreen concentration (ng/uL)"
FROM Project."Genetics Portal".samples.Biospecimens AS BS
LEFT JOIN (
  SELECT Data.SampleID,
  Data.Run."CleanUpDate",
  Data.PostCleanVol
  FROM Project."Genetics Portal".assay.General."Cleanup".Data
  JOIN (
    SELECT SampleID, MAX(Data.Run."CleanUpDate") AS "CleanUpDate" FROM Project."Genetics Portal".assay.General."Cleanup".Data
    WHERE Data.Run.Replaced IS FALSE
    GROUP BY SampleID
  ) AS MaxRun
  ON MaxRun.SampleID = Data.SampleID
  WHERE Data.Run."CleanUpDate" = MaxRun."CleanUpDate"
  AND Data.Run.Replaced IS FALSE
  GROUP BY Data.SampleID,Data.Run."CleanUpDate",Data.PostCleanVol
) AS MostRecentCleanup
ON BS.Name = MostRecentCleanup.SampleID
LEFT JOIN (
  SELECT Data."Sample ID",
  Data.Run."Date run",
  nReads,
  AvgDNA,
  Avg260280,
  Avg260230,
  StdDNA,
  Std260280,
  Std260230,
  MedDNA,
  Med260280,
  Med260230
  FROM Project."Genetics Portal".assay.General."NanoDrop QC".Data
  JOIN (
    SELECT "Sample ID",MAX(Data.Run."Date run") AS "Date run" FROM Project."Genetics Portal".assay.General."NanoDrop QC".Data
    WHERE Data.Run.Replaced IS FALSE
    GROUP BY "Sample ID"
  ) AS MaxRun
  ON MaxRun."Sample ID" = Data."Sample ID"
  WHERE Data.Run."Date run" = MaxRun."Date run"
  AND Data.Run.Replaced IS FALSE
  GROUP BY Data."Sample ID",Data.Run."Date run",nReads,AvgDNA,Avg260280,Avg260230,StdDNA,Std260280,Std260230,MedDNA,Med260280,Med260230
) AS MostRecentNDQC
ON BS.Name = MostRecentNDQC."Sample ID"
LEFT JOIN (
  SELECT Data.SampleID,
  Data.Run."Date run",
  BandVis,
  DNAdegr,
  ProtContam,
  RNAcontam,
  PassesGelQC
  FROM Project."Genetics Portal".assay.General."Gel QC".Data
  JOIN (
    SELECT SampleID, MAX(Data.Run."Date run") AS "Date run" FROM Project."Genetics Portal".assay.General."Gel QC".Data
    WHERE Data.Run.Replaced IS FALSE
    GROUP BY SampleID
  ) AS MaxRun
  ON MaxRun.SampleID = Data.SampleID
  WHERE Data.Run."Date run" = MaxRun."Date run"
  AND Data.Run.Replaced IS FALSE
) AS MostRecentGelQC
ON BS.Name = MostRecentGelQC.SampleID
LEFT JOIN (
  SELECT Data.SampleID,
  Data.Run."Date run",
  AVG(Conc) AS Conc
  FROM Project."Genetics Portal".assay.General."PicoGreen QC".Data
  JOIN (
    SELECT SampleID, MAX(Data.Run."Date run") AS "Date run" FROM Project."Genetics Portal".assay.General."PicoGreen QC".Data
    WHERE Data.Run.Replaced IS FALSE
    GROUP BY SampleID
  ) AS MaxRun
  ON MaxRun.SampleID = Data.SampleID
  WHERE Data.Run."Date run" = MaxRun."Date run"
  AND Data.Run.Replaced IS FALSE
  GROUP BY Data.SampleID,Data.Run."Date run"
) AS MostRecentPGQC
ON BS.Name = MostRecentPGQC.SampleID
LEFT JOIN Project.study.Enrollment
ON Enrollment.SubjectID = BS."Participant Id"
WHERE MostRecentNDQC."Sample ID" IS NOT NULL
OR MostRecentGelQC.SampleID IS NOT NULL
OR MostRecentPGQC.SampleID IS NOT NULL
