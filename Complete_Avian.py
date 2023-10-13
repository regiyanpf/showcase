import csv
import requests
from requests.models import HTTPBasicAuth
import json
import threading

CALLBACK_URL = "http://locuscb.tirtakencana.com/locuscb/GetDataOrders.php"
#CALLBACK_URL = "https://webhook.site/71fee821-3eea-4418-91a2-7c59823dd787"
MAX_THREADS = 12


def GetOrder(oId, failed_orders):
    try:
        GET_ORDER_ENDPOINT = "https://oms.locus-api.com/v1/client/{}/order/{}".format(
            "avian", oId
        )
        headers = {"Content-type": "application/json"}
        getorder_response = requests.get(
            GET_ORDER_ENDPOINT,
            auth=HTTPBasicAuth("mara/personnel/regi", "password"),
            headers=headers,
        )
        if getorder_response.status_code == 200:
            orderData = getorder_response.json()

            completed_updates = [
                update
                for update in orderData["history"][0]["statusUpdates"]
                if update.get("orderStatus", "") == "COMPLETED"
            ]
            
            complete_FailedRootCallbackbody = {
                "orderUpdateEventType": {"type": "ORDER_STATUS_UPDATE"},
                "order": {
                    "id": oId,
                    "sourceOrderId": orderData["sourceOrderId"],
                    "type": "DROP",
                    "date": orderData["date"],
                    "slot": orderData["slot"],
                    "channel": "TRACK_IQ",
                    "orderStatus": "COMPLETED",
            
                    # SEND FOR BOTH completed AND INBOUND_COMPLETED
                    "checklist": completed_updates[0]["orderMetadata"][
                        "checklist"
                    ],
                    "actor": completed_updates[0]["orderMetadata"]["actor"],
                    "triggerTime": completed_updates[0]["orderMetadata"][
                        "triggerTime"
                    ],
                    "latLng": {
                        "lat": completed_updates[0]["orderMetadata"]["latLng"][
                            "lat"
                        ],
                        "lng": completed_updates[0]["orderMetadata"]["latLng"][
                            "lng"
                        ],
                    },
                   
                    "lineItemTransactionStatuses": completed_updates[0][
                        "orderMetadata"
                    ]["lineItems"],
                    "teamId": orderData["teamId"],
                    "homebaseId": orderData["homebaseId"],
                    "homebaseEta": completed_updates[0]["orderMetadata"][
                        "homebaseEta"
                    ],
                    "locationId": orderData["locationId"],
                    "planIteration": completed_updates[0]["orderMetadata"][
                        "planIteration"
                    ],
                    "tourDetail": completed_updates[0]["orderMetadata"][
                        "tourDetail"
                    ],
                    "initialEta": completed_updates[0]["orderMetadata"][
                        "initialEta"
                    ],
                    "initialEtd": completed_updates[0]["orderMetadata"][
                        "initialEtd"
                    ],
                    "currentEta": completed_updates[0]["orderMetadata"][
                        "currentEta"
                    ],
                    "slaStatus": completed_updates[0]["orderMetadata"][
                        "slaStatus"
                    ],
                    "trackingInfo": completed_updates[0]["orderMetadata"][
                        "trackingInfo"
                    ],
                    "payments": {
			"payments": [],
			"fullAmountRequired": False
		},
                    "drift": completed_updates[0]["orderMetadata"]["drift"],
                },
                "timestamp": completed_updates[0]["updatedOn"],
            }

            
            completedcallbackResponse = requests.post(
                CALLBACK_URL,
                auth=HTTPBasicAuth(
                    "locus/avian", "token_here"
                ),
                json=complete_FailedRootCallbackbody,
                headers=headers,
            )

            

            if (
               completedcallbackResponse.status_code == 200
            ):
                print(
                    "\n Completed Callback Sent successfully for OrderID: " + oId
                )
                
                
        else:
            print(getorder_response.json())
            failed_orders.append(oId)
    except Exception as e:
        print(f"Exception occurred while processing OrderID: {oId}")
        failed_orders.append(oId)


def process_orders(order_ids):
    failed_orders = []
    threads = []
    for oId in order_ids:
        thread = threading.Thread(target=GetOrder, args=(oId, failed_orders))
        threads.append(thread)
        thread.start()
        if len(threads) >= MAX_THREADS:
            for thread in threads:
                thread.join()
            threads = []

    for thread in threads:
        thread.join()

    if failed_orders:
        with open("failed_orders_17-20feb.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["OrderID"])
            writer.writerows([[oId] for oId in failed_orders])
        print(f"Failed OrderIDs have been dumped to failed_orders.csv")


def main():
    order_ids = []
    with open("avian_orders.csv", "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # skip the headers
        for row in csv_reader:
            order_ids.append(row[0])

    process_orders(order_ids)


if __name__ == "__main__":
    main()
