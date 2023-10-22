#include <SPI.h>
#include <RH_RF95.h>
#include "Sodaq_DS3231.h"
#include <Wire.h>

#define RTCINTPIN 6
char Ctimestamp[13] = "";
char received[250];

char msg_got[250];

//for m0
#define RFM95_CS 8
#define RFM95_RST 4
#define RFM95_INT 3
#define RF95_FREQ 433.0

RH_RF95 rf95(RFM95_CS, RFM95_INT);

void setup() 
{
  Serial.begin(115200);
  while (!Serial){
    delay(1); // Wait for serial port to be available
  }
  
  pinMode(RFM95_RST, OUTPUT);
  digitalWrite(RFM95_RST, HIGH);

  Wire.begin();
  rtc.begin();

  // manual reset
  digitalWrite(RFM95_RST, LOW);
  delay(10);
  digitalWrite(RFM95_RST, HIGH);
  delay(10);

  while (!rf95.init()) 
  {
//    Serial.println("LoRa radio init failed");
    while (1);
  }
//  Serial.println("LoRa radio init OK!");

  // Defaults after init are 434.0MHz, modulation GFSK_Rb250Fd250, +13dbM
  if (!rf95.setFrequency(RF95_FREQ)) 
  {
    Serial.println("setFrequency failed");
    while (1);
  }  
  
  rf95.setModemConfig(RH_RF95::Bw125Cr45Sf128);
}


void readTimeStamp()
{
    DateTime now = rtc.now(); //get the current date-time
    String ts = String(now.year());

    if (now.month() <= 9) {
        ts += "0" + String(now.month());
    } else {
        ts += String(now.month());
    }
    if (now.date() <= 9) {
        ts += "0" + String(now.date());
    } else {
        ts += String(now.date());
    }
    if (now.hour() <= 9) {
        ts += "0" + String(now.hour());
    } else {
        ts += String(now.hour());
    }
    if (now.minute() <= 9) {
        ts += "0" + String(now.minute());
    } else {
        ts += String(now.minute());
    }
    if (now.second() <= 9) {
        ts += "0" + String(now.second());
    } else {
        ts += String(now.second());
    }

    ts.remove(0, 2); //remove 1st 2 data in ts
    ts.toCharArray(Ctimestamp, 13);
}


void loop()
{
  uint8_t buf[RH_RF95_MAX_MESSAGE_LEN];
  uint8_t len2 = sizeof(buf);

//  received[0] = 0x00;  
    
  if (rf95.available())
  {
    if (rf95.recv(buf, &len2))
    {    
        int i = 0;
        for (i = 0; i < len2; ++i) {
          received[i] = (uint8_t)buf[i];
        }
        received[i] = (uint8_t)'\0';
        
//        int j = 0;
//        for (j = 0; j < len; j++) {
//          msg_got[j] = (uint8_t)received[j];
//        }
//        msg_got[j] = (uint8_t)'\0';
         
        // readTimeStamp();
        // strcat(received,"*");
        // strncat(received,Ctimestamp,13);
        Serial.println(received);
                               
    // } else {
    //   Serial.println("CAD timeout");
    }
  }
}
