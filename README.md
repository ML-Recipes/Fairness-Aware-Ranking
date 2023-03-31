# Fairness-Aware-Ranking

Online marketplace platforms are emerging in various domains e-commerce (e.g., Amazon or eBay), freelancing (e.g., Upwork or Fiverr), short-term home renting (e.g., AirBNB). When designing such platforms, bias can be found in multiple blocks of a system architecture, such as, (1) data gathering or ground-truth collection (2) biased algorithms or (3) biased results presented to the users. If bias is not detected or handled properly, the algorithm learns from this information and generates biased results.

Considering fairness constraints, when designing and engineering such systems by identifying bias and promoting fairness, not only helps improve users’ engagement into the system, but also helps businesses increase marketing and revenue. In this project, we aim to study the countering bias in search and recommender systems, namely, understanding data bias and considering fairness in ranking methods. To facilitate our study, we used an AirBNB dataset and developed a search system using ElasticSearch (ES). First, we analyzed the initial ranking results to identify marketplace inequality. Finally, we implemented a fairness-aware ranking based on the idea of “impression redistribution”.


