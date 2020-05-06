# pscAnD
Anomaly Detection of System Logs at Pittsburgh Supercomputing Center. 

This work was published in a conference - Machine Learning for Computing Sciences, Tempe, Arizona. The research can be accessed here:
Paper link: https://dl.acm.org/ft_gateway.cfm?id=3217873&type=pdf
ACM Citation link: https://dl.acm.org/citation.cfm?id=3217873
Conference Link: https://mlcsworkshop.weebly.com/previous-editions-2018.html

The contents of this repository are as follows:
1) nfsTimeOutMatrix.ipynb contains analysis of timeouts at different nodes of the system

2) OutOfJObNFSErrorAnalysis.ipynb contains analysis of timeouts on a node which was not performing a job

3) training_model.ipynb contains making a corpus out of the system logs using a sliding window technique. The corpus is broken to create samples by checking if within a time-period(configurable) an error has been encountered in the system. The corpuses that encounter an error are tagged as positive samples adn the ones that dont are negative samples. 

The dataset created is imbalanced as expected.

4) ROCAnalysis.ipynb contains multiple scenarios with different ratios of positive and negative samples and ROC curves obtained in each.
Training on 100 Positive and 100 negative samples

![100 Positive and 100 Negative](/ROC-bridges-Images/case0.png)

Training on 120 Positive and 120 Negative samples

![120 Positive and 120 Negative](/ROC-bridges-Images/case2.png)

Training on 120 Positive and 120 Negative samples and testing is done on samples from logs of a different date

![60 Positive 60 Negative Test on Different Date](/ROC-bridges-Images/case3.png)

Training on 120 Positive and 120 Negative samples and testing is done on samples from logs of same date but different node

![60 Positive 60 Negative Test on Same Date Different Node](/ROC-bridges-Images/case41.png)
