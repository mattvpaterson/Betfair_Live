# Betfair_Live

This repo contains work done on developing a functional trading/ordering system for implementation of betfair exchange trading strategies developed within the Betfair_Static repo. The following objects/structures need to be built:

-Meta_Data_Handler
	interaction with the betting API to obtain and store relevant listing reference data
-Market_Data_Handler
	interaction with the exchange streaming API for marketdata management and order book construction
-Strategy
	construction of features from current and past order book snapshots, evaluation of strategy signals and instructing orders to the order management system
-Order_Management_System
	system that keeps tracks of order submission/existence/fills/cancellation, needs to interact with account management 
-Account_Management_System
	checks account status (using accounts API), sets ordering limits order submission passes throught account management before execution
-Storage
	database storage of enriched trade and ordering data (it is beyond the scope of this project for storage of extensive packet data, for this we use betfair storage dumps as in Betfair_Static


So far progress has only been made on implementing Meta_Data_Handler, check Populating_Reference_Data.ipynb for details.
The most difficult/unfamiliar part of the project for me will be the Market_Data_Handler, source code is only available in C#, Java & Node.js.
https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/Exchange+Stream+API
All other parts I think can be done straightforwardly in python as strategies that we will test will not be ultra low latency sensitive.


