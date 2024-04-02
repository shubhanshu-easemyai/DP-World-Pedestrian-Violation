Resolved error of roi details of each loaded camera.
Now the id's are used to extract values defined by user from the respective camera details to process the thresholds required in the processing.

This is draft of the Pedestrian-Violation detection.

cache of checker will have all the detected object in a ROI
cache of checkers_id is by extracting the id(s) from the checker

Pedestrian-Violation detection - it shows all the detected person in the roi if violator(s) is(are) there. DP world require a report generated in report_generated which now have all the data points of the object that is present in the ROI (although when we have the information of specific object required then we will filter out the required object(s))