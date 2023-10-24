#include <Arduino.h>   // required before wiring_private.h
#include "wiring_private.h" // pinPeripheral() function
#include <SPI.h>
#include <RH_RF95.h>
#include <SparkFun_u-blox_GNSS_Arduino_Library.h>
#include "Sodaq_DS3231.h"
#include <Wire.h>
#include <LowPower.h>
#include <EnableInterrupt.h>
#include <Adafruit_SleepyDog.h>
SFE_UBLOX_GNSS myGNSS;

#define BUFLEN (5*RH_RF95_MAX_MESSAGE_LEN) //max size of data burst we can handle - (5 full RF buffers) - just arbitrarily large
#define RFWAITTIME 500 //maximum milliseconds to wait for next LoRa packet - used to be 600 - may have been too long
#define rtcm_timeout 180000 //3 minutes

char sitecode[6] = "TESUA"; //logger name - sensor site code
int min_sat = 29;
int loop_counter = 5;
int ave_count = 12;

char dataToSend[200];
char Ctimestamp[13] = "";

uint16_t store_rtc = 00; //store rtc alarm
volatile bool OperationFlag = false;
bool read_flag = false;
uint8_t rx_lora_flag = 0;

// initialize LoRa global variables
uint8_t payload[RH_RF95_MAX_MESSAGE_LEN];
uint8_t len = sizeof(payload);
uint8_t buf[RH_RF95_MAX_MESSAGE_LEN];
uint8_t len2 = sizeof(buf);   

#define DEBUG 1
#define RTCINTPIN 6
#define VBATPIN A7    //new copy
#define VBATEXT A5

// for feather m0
#define RFM95_CS 8
#define RFM95_RST 4
#define RFM95_INT 3
#define RF95_FREQ 433.0
RH_RF95 rf95(RFM95_CS, RFM95_INT);

#define LED 13
unsigned long start;

// We will use Serial2 - Rx on pin 11, Tx on pin 10
Uart Serial2 (&sercom1, 11, 10, SERCOM_RX_PAD_0, UART_TX_PAD_2);

void SERCOM1_Handler() {
  Serial2.IrqHandler();
}

void init_ublox() {
  Wire.begin();
  if (myGNSS.begin(Wire) == false) {
    Serial.println(F("u-blox GNSS not detected at default I2C address. Please check wiring. Freezing."));
    while (1);
  }
  myGNSS.setI2COutput(COM_TYPE_UBX); //Set the I2C port to output UBX only (turn off NMEA noise)
  myGNSS.setNavigationFrequency(5); //Set output to 20 times a second

  myGNSS.setHighPrecisionMode(true);  
  myGNSS.powerSaveMode(true);
}

void setup() {
  Serial.begin(115200);
  Serial2.begin(115200);

  // Assign pins 10 & 11 SERCOM functionality
  pinPeripheral(10, PIO_SERCOM);
  pinPeripheral(11, PIO_SERCOM);
  delay(100);
  
  Wire.begin();
  rtc.begin();

  pinMode(LED, OUTPUT);
  pinMode(RFM95_RST, OUTPUT);
  digitalWrite(RFM95_RST, HIGH);

  digitalWrite(RFM95_RST, LOW);
  delay_millis(10);
  digitalWrite(RFM95_RST, HIGH);
  delay_millis(10);

  Serial.println("Feather LoRa RX");
  digitalWrite(RFM95_RST, LOW);
  delay(10);
  digitalWrite(RFM95_RST, HIGH);
  delay(10);

  while (!rf95.init()) {
    Serial.println("LoRa radio init failed");
    while (1);
  } Serial.println("LoRa radio init OK!");

  if (!rf95.setFrequency(RF95_FREQ)) {
    Serial.println("setFrequency failed");
    while (1);
  }

  rf95.setTxPower(23, false);
  init_ublox();
}

void readTimeStamp() {
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

float readTemp() {
  float temp;
  rtc.convertTemperature();
  temp = rtc.getTemperature();
  return temp;
}

void delay_millis(int _delay) {
  uint8_t delay_turn_on_flag = 0;
  unsigned long _delayStart = millis();

  do {
    if ((millis() - _delayStart) > _delay) {
      _delayStart = millis();
      delay_turn_on_flag = 1;
    }
  } while (delay_turn_on_flag == 0);
}

void send_thru_lora(char* radiopacket) {
    rf95.setModemConfig(RH_RF95::Bw125Cr45Sf128);
    uint8_t payload[RH_RF95_MAX_MESSAGE_LEN];
    int len = String(radiopacket).length();
    int i=0, j=0;
    memset(payload,'\0',251);

    Serial.println("Sending to rf95_server");

    //do not stack
    for(i=0; i<len; i++) {
      payload[i] = (uint8_t)'0';
    }
    
    for(i=0; i<len; i++) {
      payload[i] = (uint8_t)radiopacket[i];
    }
    payload[i] = (uint8_t)'\0';
    
    Serial.println((char*)payload);
    Serial.println("sending payload!");
    rf95.send(payload, len);
    rf95.waitPacketSent();
    delay(100);  
}

float readBatteryVoltage(uint8_t ver) {
  float measuredvbat;
  if ((ver == 3) || (ver == 9) || (ver == 10) || (ver == 11)) {
    measuredvbat = analogRead(VBATPIN); //Measure the battery voltage at pin A7
    measuredvbat *= 2;                  // we divided by 2, so multiply back
    measuredvbat *= 3.3;                // Multiply by 3.3V, our reference voltage
    measuredvbat /= 1024;               // convert to voltage
    measuredvbat += 0.28;               // add 0.7V drop in schottky diode
  } else {
    /* Voltage Divider 1M and  100k */
    measuredvbat = analogRead(VBATEXT);
    measuredvbat *= 3.3;                // reference voltage
    measuredvbat /= 1024.0;             // adc max count
    measuredvbat *= 11.0;               // (100k+1M)/100k
  }
  return measuredvbat;
}

byte RTK() {
  byte RTK = myGNSS.getCarrierSolutionType();
  Serial.print("RTK: ");
  Serial.println(RTK);
  if (RTK == 0) Serial.println(F(" (No solution)"));
  else if (RTK == 1) Serial.println(F(" (High precision floating fix)"));
  else if (RTK == 2) Serial.println(F(" (High precision fix)"));
  return RTK;
}

byte SIV() {
  byte SIV = myGNSS.getSIV();
  Serial.print("Satellite count: ");
  Serial.println(SIV);
  return SIV;
}

float HACC() {
  float HACC = myGNSS.getHorizontalAccuracy();
  Serial.print("Horizontal Accuracy: ");
  Serial.println(HACC);
  return HACC;
}

float VACC() {
  float VACC = myGNSS.getVerticalAccuracy();
  Serial.print("Vertical Accuracy: ");
  Serial.println(VACC);
  return VACC;
}

void get_rtcm() {
  rf95.setModemConfig(RH_RF95::Bw500Cr45Sf128);   //lora config for send/receive rtcm
  uint8_t buf[BUFLEN];
  unsigned buflen;

  uint8_t rfbuflen;
  uint8_t *bufptr;
  unsigned long lastTime, curTime;

  bufptr = buf;
  if (rf95.available()) {
    digitalWrite(LED, HIGH);
    rfbuflen = RH_RF95_MAX_MESSAGE_LEN;
    if (rf95.recv(bufptr, &rfbuflen)) {
      bufptr += rfbuflen;
      lastTime = millis();
      while (((millis() - lastTime) < RFWAITTIME) && ((bufptr - buf) < (BUFLEN - RH_RF95_MAX_MESSAGE_LEN))) { //Time out or buffer can't hold anymore
        if (rf95.available()) {
          rfbuflen = RH_RF95_MAX_MESSAGE_LEN;
          if (rf95.recv(bufptr, &rfbuflen)) {
            Serial.println((unsigned char) *bufptr, HEX);
            bufptr += rfbuflen;
            lastTime = millis();
          } else {
            Serial.println("Receive failed");
          }
        }
      }
    } else {
      Serial.println("Receive failed");
    }
    buflen = (bufptr - buf);     //Total bytes received in all packets
    Serial2.write(buf, buflen); //Send data to the GPS
    digitalWrite(LED, LOW);
  }
}

// void read_ubx() {
//   for (int i = 0; i < 200; i++) {
//     dataToSend[i] = 0x00;
//   }
//   memset(dataToSend,'\0',200);


//   Serial.print("relPosN: ");
//   Serial.println(myGNSS.getRelPosN(), 4); // Use the helper functions to get the rel. pos. as m
//   Serial.print("relPosE: ");
//   Serial.println(myGNSS.getRelPosE(), 4);
//   Serial.print("relPosD: ");
//   Serial.println(myGNSS.getRelPosD(), 4);

//   Serial.print("relPosLength: ");
//   Serial.println(myGNSS.packetUBXNAVRELPOSNED->data.relPosLength);
//   Serial.print("relPosHeading: ");
//   Serial.println(myGNSS.packetUBXNAVRELPOSNED->data.relPosHeading);

//   //high precision
//   double relposhpN = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPN;
//   double relposhpE = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPE;
//   double relposhpD = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPD;
//   double relposhpLEN = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPLength;
  
//   //helper functions for relposned
//   // float relposN = myGNSS.getRelPosN();    // Returned as m
//   // float relposE =  myGNSS.getRelPosE();    // Returned as m
//   // float relposD =  myGNSS.getRelPosD();    // Returned as m

//   float relposaccN =  myGNSS.getRelPosAccN(); // Returned as m
//   float relposaccE =  myGNSS.getRelPosAccE(); // Returned as m
//   float relposaccD =  myGNSS.getRelPosAccD(); // Returned as m

//   Serial.print("relPosHPN: "); Serial.println(relposhpN);
//   Serial.print("relPosHPE: "); Serial.println(relposhpE);
//   Serial.print("relPosHPD: "); Serial.println(relposhpD);
//   Serial.print("relPosHPLength: "); Serial.println(relposhpLEN);

//   Serial.print("accN: "); Serial.println(relposaccN);
//   Serial.print("accE: "); Serial.println(relposaccE);
//   Serial.print("accD: "); Serial.println(relposaccD);




//   sprintf(tempstr_d, "double_%s:%d,%.9f,%.9f,%.4f,%.4f,%.4f,%d", sitecode, rtk_fixtype, d_lat, d_lon, f_accuracy_hor_d, f_accuracy_ver_d, f_msl, sat_num_d);
//   strncpy(dataToSend, tempstr_d, String(tempstr_d).length() + 1);
//   strncat(dataToSend, ",", 2);
//   strncat(dataToSend, temp_d, sizeof(temp_d));
//   strncat(dataToSend, ",", 2);
//   strncat(dataToSend, volt_d, sizeof(volt_d)); 

//   readTimeStamp();
//   strncat(dataToSend, "*", 2);
//   strncat(dataToSend, Ctimestamp, 13);
//   Serial.print("data to send: "); Serial.println(dataToSend);
// }


void read_ubx() {
  for (int i = 0; i < 200; i++) {
    dataToSend[i] = 0x00;
  }
  memset(dataToSend,'\0',200);

  byte rtk_fixtype = RTK();
  char tempstr_d[100];

  //high precision
  double relposhpN = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPN;
  double relposhpE = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPE;
  double relposhpD = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPD;
  double relposhpLEN = myGNSS.packetUBXNAVRELPOSNED->data.relPosHPLength;
  double relposHEAD = myGNSS.packetUBXNAVRELPOSNED->data.relPosHeading;

  float relposaccN =  myGNSS.getRelPosAccN(); // Returned as m
  float relposaccE =  myGNSS.getRelPosAccE(); // Returned as m
  float relposaccD =  myGNSS.getRelPosAccD(); // Returned as m

  Serial.print("relPosHPN: "); Serial.println(relposhpN);
  Serial.print("relPosHPE: "); Serial.println(relposhpE);
  Serial.print("relPosHPD: "); Serial.println(relposhpD);
  Serial.print("relPosHPLength: "); Serial.println(relposhpLEN);
  Serial.print("relPosHeading: "); Serial.println(relposHEAD);

  Serial.print("accN: "); Serial.println(relposaccN);
  Serial.print("accE: "); Serial.println(relposaccE);
  Serial.print("accD: "); Serial.println(relposaccD);


  sprintf(tempstr_d, "%s:%d,%f,%f,%f,%f,%f,%f,%f,%f", sitecode, rtk_fixtype, relposhpN, relposhpE, relposhpD, relposhpLEN, relposHEAD, relposaccN, relposaccE, relposaccD);
  strncpy(dataToSend, tempstr_d, String(tempstr_d).length() + 1);

  readTimeStamp();
  strncat(dataToSend, "*", 2);
  strncat(dataToSend, Ctimestamp, 13);
  Serial.print("data to send: "); Serial.println(dataToSend);
}



void loop() {
  get_rtcm();

  if (RTK() == 2 && SIV() >= min_sat) {
   if (HACC() == 141) {
      get_rtcm();
      read_ubx();
      send_thru_lora(dataToSend);
   }  
  }
}
