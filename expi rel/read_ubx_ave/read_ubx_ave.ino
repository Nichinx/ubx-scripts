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
  attachInterrupt(RTCINTPIN, wake, FALLING);
  init_Sleep(); //initialize MCU sleep state
  setAlarmEvery30(1); //rtc alarm settings

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
  readTimeStamp();
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

//void read_ubx_in_double() {
//  for (int i = 0; i < 200; i++) {
//    dataToSend[i] = 0x00;
//  }
//  memset(dataToSend,'\0',200);
//
//  byte rtk_fixtype = RTK();
//  int sat_num_d = SIV();
//
//  char tempstr_d[100];
//  char volt_d[10];
//  char temp_d[10];
//
//  snprintf(volt_d, sizeof volt_d, "%.2f", readBatteryVoltage(10));
//  snprintf(temp_d, sizeof temp_d, "%.2f", readTemp());
//
//  // Defines storage for the lat and lon as double
//  double d_lat = 0.0; // latitude
//  double d_lon = 0.0; // longitude
//  
//  double accu_lat = 0.0; // latitude
//  double accu_lon = 0.0; // longitude
//
//  // Now define float storage for the heights and accuracy
//  float f_msl = 0.0;
//  float f_accuracy_hor_d = 0.0;
//  float f_accuracy_ver_d = 0.0;
//
//  float accu_msl = 0.0;
//  float accu_accuracy_hor_d = 0.0;
//  float accu_accuracy_ver_d = 0.0;
//
//  for (int i = 1; i <= ave_count; i++) {
//    get_rtcm();
//    
//    // First, let's collect the position data
//    int32_t latitude = myGNSS.getHighResLatitude();
//    int8_t latitudeHp = myGNSS.getHighResLatitudeHp();
//    int32_t longitude = myGNSS.getHighResLongitude();
//    int8_t longitudeHp = myGNSS.getHighResLongitudeHp();
//    int32_t msl = myGNSS.getMeanSeaLevel();
//    int8_t mslHp = myGNSS.getMeanSeaLevelHp();
//    uint32_t hor_acc = myGNSS.getHorizontalAccuracy();
//    uint32_t ver_acc = myGNSS.getVerticalAccuracy();
//
//    // Assemble the high precision latitude and longitude
//    d_lat = ((double)latitude) / 10000000.0; // Convert latitude from degrees * 10^-7 to degrees
//    d_lat += ((double)latitudeHp) / 1000000000.0; // Now add the high resolution component (degrees * 10^-9 )
//    d_lon = ((double)longitude) / 10000000.0; // Convert longitude from degrees * 10^-7 to degrees
//    d_lon += ((double)longitudeHp) / 1000000000.0; // Now add the high resolution component (degrees * 10^-9 )
//
//    // Calculate the height above mean sea level in mm * 10^-1
//    f_msl = (msl * 10) + mslHp;
//    f_msl = f_msl / 10000.0; // Convert from mm * 10^-1 to m
//
//    // Convert the accuracy (mm * 10^-1) to a float
//    f_accuracy_hor_d = hor_acc / 10000.0; // Convert from mm * 10^-1 to m
//    f_accuracy_ver_d = ver_acc / 10000.0; // Convert from mm * 10^-1 to m
//
//    // Accumulation
//    accu_lat = accu_lat + d_lat;
//    accu_lon = accu_lon + d_lon;
//    accu_msl = accu_msl + f_msl;
//    accu_accuracy_hor_d = accu_accuracy_hor_d + f_accuracy_hor_d;
//    accu_accuracy_ver_d = accu_accuracy_ver_d + f_accuracy_ver_d;
//
//    get_rtcm();
//  }
//
//  // Averaging
//  d_lat = accu_lat / ave_count; 
//  d_lon = accu_lon / ave_count;
//  f_msl = accu_msl / ave_count; 
//  f_accuracy_hor_d = accu_accuracy_hor_d / ave_count;
//  f_accuracy_ver_d = accu_accuracy_ver_d / ave_count;
//
//  sprintf(tempstr_d, "double_%s:%d,%.9f,%.9f,%.4f,%.4f,%.4f,%d", sitecode, rtk_fixtype, d_lat, d_lon, f_accuracy_hor_d, f_accuracy_ver_d, f_msl, sat_num_d);
//  strncpy(dataToSend, tempstr_d, String(tempstr_d).length() + 1);
//  strncat(dataToSend, ",", 2);
//  strncat(dataToSend, temp_d, sizeof(temp_d));
//  strncat(dataToSend, ",", 2);
//  strncat(dataToSend, volt_d, sizeof(volt_d)); 
//
//  readTimeStamp();
//  strncat(dataToSend, "*", 2);
//  strncat(dataToSend, Ctimestamp, 13);
//  Serial.print("data to send: "); Serial.println(dataToSend);
//  get_rtcm();
//}

///////expe
void read_ubx_in_double() {
  for (int i = 0; i < 200; i++) {
    dataToSend[i] = 0x00;
  }
  memset(dataToSend,'\0',200);

  byte rtk_fixtype = RTK();
  int sat_num_d = SIV();

  int accu_count = 0;
  
  char tempstr_d[100];
  char volt_d[10];
  char temp_d[10];

  snprintf(volt_d, sizeof volt_d, "%.2f", readBatteryVoltage(10));
  snprintf(temp_d, sizeof temp_d, "%.2f", readTemp());

  // Defines storage for the lat and lon as double
  double d_lat = 0.0; // latitude
  double d_lon = 0.0; // longitude
  
  double accu_lat = 0.0; // latitude
  double accu_lon = 0.0; // longitude

  // Now define float storage for the heights and accuracy
  float f_msl = 0.0;
  float f_accuracy_hor_d = 0.0;
  float f_accuracy_ver_d = 0.0;
  
  float accu_msl = 0.0;
  float accu_accuracy_hor_d = 0.0;
  float accu_accuracy_ver_d = 0.0;

  for (int i = 0; i <= (ave_count - 1); i++) {
    get_rtcm();
    
    // First, let's collect the position data
    int32_t latitude = myGNSS.getHighResLatitude();
    int8_t latitudeHp = myGNSS.getHighResLatitudeHp();
    int32_t longitude = myGNSS.getHighResLongitude();
    int8_t longitudeHp = myGNSS.getHighResLongitudeHp();
    int32_t msl = myGNSS.getMeanSeaLevel();
    int8_t mslHp = myGNSS.getMeanSeaLevelHp();
    uint32_t hor_acc = myGNSS.getHorizontalAccuracy();
    uint32_t ver_acc = myGNSS.getVerticalAccuracy();

    // Assemble the high precision latitude and longitude
    d_lat = ((double)latitude) / 10000000.0; // Convert latitude from degrees * 10^-7 to degrees
    d_lat += ((double)latitudeHp) / 1000000000.0; // Now add the high resolution component (degrees * 10^-9 )
    d_lon = ((double)longitude) / 10000000.0; // Convert longitude from degrees * 10^-7 to degrees
    d_lon += ((double)longitudeHp) / 1000000000.0; // Now add the high resolution component (degrees * 10^-9 )

    // Calculate the height above mean sea level in mm * 10^-1
    f_msl = (msl * 10) + mslHp;
    f_msl = f_msl / 10000.0; // Convert from mm * 10^-1 to m

    // Convert the accuracy (mm * 10^-1) to a float
    f_accuracy_hor_d = hor_acc / 10000.0; // Convert from mm * 10^-1 to m
    f_accuracy_ver_d = ver_acc / 10000.0; // Convert from mm * 10^-1 to m

      // if ((HACC() == 141 && VACC() == 100) || (HACC() == 141 && VACC() <= 141)) {
      if ((HACC() == 141 && VACC() == 99)) {
        // Accumulation
        accu_lat = accu_lat + d_lat;
        accu_lon = accu_lon + d_lon;
        accu_msl = accu_msl + f_msl;
        accu_accuracy_hor_d = accu_accuracy_hor_d + f_accuracy_hor_d;
        accu_accuracy_ver_d = accu_accuracy_ver_d + f_accuracy_ver_d;
        accu_count++;
        // i++;
        Serial.print("accu_count: "); Serial.println(accu_count);
        Serial.print("iter_count: "); Serial.println(i);
      } else {
        get_rtcm();
        // i = i - 1; //loop until hacc&vacc conditions are satisfied
        i++; //wont wait for conditions to be satistied, loop continues to iterate regardless
        Serial.print("iter_count: "); Serial.println(i);

          // if ((i == (ave_count - 1)) && (VACC() > 141)) { //this if condition exists if loop chosen was 2nd else loop (loop continues to iterate regardless      
          //   // i = 0;
          //   // accu_count = 1;
          //   Serial.print("iter_count: "); Serial.println(i);
          //   break;
          // }
      } 
  }

  // Averaging
  d_lat = accu_lat / accu_count; 
  d_lon = accu_lon / accu_count;
  f_msl = accu_msl / accu_count; 
  f_accuracy_hor_d = accu_accuracy_hor_d / accu_count;
  f_accuracy_ver_d = accu_accuracy_ver_d / accu_count;

  if (d_lat != 0) { //this below is the original code:
    sprintf(tempstr_d, "double_%s:%d,%.9f,%.9f,%.4f,%.4f,%.4f,%d", sitecode, rtk_fixtype, d_lat, d_lon, f_accuracy_hor_d, f_accuracy_ver_d, f_msl, sat_num_d);
    strncpy(dataToSend, tempstr_d, String(tempstr_d).length() + 1);
    strncat(dataToSend, ",", 2);
    strncat(dataToSend, temp_d, sizeof(temp_d));
    strncat(dataToSend, ",", 2);
    strncat(dataToSend, volt_d, sizeof(volt_d)); 

    readTimeStamp();
    strncat(dataToSend, "*", 2);
    strncat(dataToSend, Ctimestamp, 13);
    Serial.print("data to send: "); Serial.println(dataToSend);
    get_rtcm();

  } else if (d_lat == 0/0) {   //else - if condition exists if 2nd loop chosen (loop continues to iterate regardless)
    sprintf(tempstr_d, "no data");
    strncpy(dataToSend, tempstr_d, String(tempstr_d).length() + 1);
    readTimeStamp();
    strncat(dataToSend, "*", 2);
    strncat(dataToSend, Ctimestamp, 13);
    Serial.print("data to send: "); Serial.println(dataToSend);
    get_rtcm();
  }
}
///////////

void no_ublox_data() {
  for (int i = 0; i < 200; i++){
    dataToSend[i] = 0x00;  
  } 
  memset(dataToSend,'\0',200);
  
  char ndstr[100];
  char ND[14] = "No Ublox data";

  sprintf(ndstr, ">>%s:%s", sitecode, ND);
  strncat(dataToSend, ndstr, sizeof(ndstr));
  Serial.print("data to send: "); Serial.println(dataToSend);
}

////10.07.23 - averaging
void loop() {
  get_rtcm();

  if (RTK() == 2 && SIV() >= min_sat) {
//    if (HACC() == 141) {
      read_ubx_in_double();
      send_thru_lora(dataToSend);
//    }  
  }
}

//////10.22.23 - averaging with on/off
//void loop() {
//  start = millis();
//  rx_lora_flag = 0;
//
//  do {
//    get_rtcm();
//  } while (((RTK() != 2) || SIV() < min_sat) && ((millis() - start) < rtcm_timeout)); 
//
//  if (RTK() == 2 && SIV() >= min_sat) {
//    if (rx_lora_flag == 0) {
//      if (HACC() == 141) {
//        read_ubx_in_double();
//        rx_lora_flag == 1;
//        read_flag = true;
//      }
//    }
//
//  } else if ((RTK() != 2) || (SIV() < min_sat) || ((millis() - start) >= rtcm_timeout)) {
//      Serial.println("Unable to obtain fix or no. of satellites reqd. not met");
//      no_ublox_data();     
//      rx_lora_flag == 1;
//      read_flag = true;
//  }  
//
//  if (read_flag = true) {
//      read_flag = false;
//      rx_lora_flag == 0;
//      send_thru_lora(dataToSend);
//  }
//
//  attachInterrupt(RTCINTPIN, wake, FALLING);
//  setAlarmEvery30(1);
//  rtc.clearINTStatus();
//  sleepNow();
//}

//30 minutes interval
void setAlarmEvery30(int alarmSET)
{
  DateTime now = rtc.now(); //get the current date-time
  switch (alarmSET)
  {
    case 0:
      {
        //set every 10 minutes interval
        if ((now.minute() >= 1) && (now.minute() <= 10))
        {
          store_rtc = 10;
        }
        else if ((now.minute() >= 11) && (now.minute() <= 20))
        {
          store_rtc = 20;
        }
        else if ((now.minute() >= 21) && (now.minute() <= 30))
        {
          store_rtc = 30;
        }
        else if ((now.minute() >= 31) && (now.minute() <= 40))
        {
          store_rtc = 40;
        }
        else if ((now.minute() >= 41) && (now.minute() <= 50))
        {
          store_rtc = 50;
        }
        else if ((now.minute() >= 51) && (now.minute() <= 0))
        {
          store_rtc = 0;
        }
        enable_rtc_interrupt();
        break;
      }
    case 1: //5 mins interval
      {
        if ((now.minute() >= 0) && (now.minute() <= 4))
        {
          store_rtc = 5;
        }
        else if ((now.minute() >= 5) && (now.minute() <= 9))
        {
          store_rtc = 10;
        }
        else if ((now.minute() >= 10) && (now.minute() <= 14))
        {
          store_rtc = 15;
        }
        else if ((now.minute() >= 15) && (now.minute() <= 19))
        {
          store_rtc = 20;
        }
        else if ((now.minute() >= 20) && (now.minute() <= 24))
        {
          store_rtc = 25;
        }
        else if ((now.minute() >= 25) && (now.minute() <= 29))
        {
          store_rtc = 30;
        }
        else if ((now.minute() >= 30) && (now.minute() <= 34))
        {
          store_rtc = 35;
        }
        else if ((now.minute() >= 35) && (now.minute() <= 39))
        {
          store_rtc = 40;
        }
        else if ((now.minute() >= 40) && (now.minute() <= 44))
        {
          store_rtc = 45;
        }
        else if ((now.minute() >= 45) && (now.minute() <= 49))
        {
          store_rtc = 50;
        }
        else if ((now.minute() >= 50) && (now.minute() <= 54))
        {
          store_rtc = 55;
        }
        else if ((now.minute() >= 55) && (now.minute() <= 59))
        {
          store_rtc = 0;
        }
        enable_rtc_interrupt();
        break;
      }
    case 2: //3 mins interval
      {
        if ((now.minute() >= 0) && (now.minute() <= 2)) {
          store_rtc = 3;
        } else if ((now.minute() >= 3) && (now.minute() <= 5)) {
          store_rtc = 6;
        } else if ((now.minute() >= 6) && (now.minute() <= 8)) {
          store_rtc = 9;
        } else if ((now.minute() >= 9) && (now.minute() <= 11)) {
          store_rtc = 12;
        } else if ((now.minute() >= 12) && (now.minute() <= 14)) {
          store_rtc = 15;
        } else if ((now.minute() >= 15) && (now.minute() <= 17)) {
          store_rtc = 18;
        } else if ((now.minute() >= 18) && (now.minute() <= 20)) {
          store_rtc = 21;
        } else if ((now.minute() >= 21) && (now.minute() <= 23)) {
          store_rtc = 24;
        } else if ((now.minute() >= 24) && (now.minute() <= 26)) {
          store_rtc = 27;
        } else if ((now.minute() >= 27) && (now.minute() <= 29)) {
          store_rtc = 30;
        } else if ((now.minute() >= 30) && (now.minute() <= 32)) {
          store_rtc = 33;
        } else if ((now.minute() >= 33) && (now.minute() <= 35)) {
          store_rtc = 36;
        } else if ((now.minute() >= 36) && (now.minute() <= 38)) {
          store_rtc = 39;
        } else if ((now.minute() >= 39) && (now.minute() <= 41)) {
          store_rtc = 42;
        } else if ((now.minute() >= 42) && (now.minute() <= 44)) {
          store_rtc = 45;
        } else if ((now.minute() >= 45) && (now.minute() <= 47)) {
          store_rtc = 48;
        } else if ((now.minute() >= 48) && (now.minute() <= 50)) {
          store_rtc = 51;
        } else if ((now.minute() >= 51) && (now.minute() <= 53)) {
          store_rtc = 54;
        } else if ((now.minute() >= 54) && (now.minute() <= 56)) {
          store_rtc = 57;
        } else if ((now.minute() >= 57) && (now.minute() <= 59)) {
          store_rtc = 0;
        }
        enable_rtc_interrupt();
        break;
      }
    case 3:  //set every 10 minutes interval
      {
        if ((now.minute() >= 0) && (now.minute() <= 9)) {
          store_rtc = 10;
        } else if ((now.minute() >= 10) && (now.minute() <= 19)) {
          store_rtc = 20;
        } else if ((now.minute() >= 20) && (now.minute() <= 29)) {
          store_rtc = 30;
        } else if ((now.minute() >= 30) && (now.minute() <= 39)) {
          store_rtc = 40;
        } else if ((now.minute() >= 40) && (now.minute() <= 49)) {
          store_rtc = 50;
        } else if ((now.minute() >= 50) && (now.minute() <= 59)) {
          store_rtc = 0;
        }
        enable_rtc_interrupt();
        break;
      }  
  }
}

void enable_rtc_interrupt()
{
  rtc.enableInterrupts(store_rtc, 00); // interrupt at (minutes, seconds)
  if (DEBUG == 1) {
    Serial.print("Next alarm: ");
  }
  if (DEBUG == 1) {
    Serial.println(store_rtc);
  }
  readTimeStamp();
}

void wake()
{
  // OperationFlag = true;
  //detach the interrupt in the ISR so that multiple ISRs are not called
  detachInterrupt(RTCINTPIN);
  Watchdog.reset();
}

void init_Sleep()
{
  //working to as of 05-17-2019
  SYSCTRL->XOSC32K.reg |= (SYSCTRL_XOSC32K_RUNSTDBY | SYSCTRL_XOSC32K_ONDEMAND); // set external 32k oscillator to run when idle or sleep mode is chosen
  REG_GCLK_CLKCTRL |= GCLK_CLKCTRL_ID(GCM_EIC) |                                 // generic clock multiplexer id for the external interrupt controller
                      GCLK_CLKCTRL_GEN_GCLK1 |                                   // generic clock 1 which is xosc32k
                      GCLK_CLKCTRL_CLKEN;                                        // enable it
  while (GCLK->STATUS.bit.SYNCBUSY)
    ; // write protected, wait for sync

  EIC->WAKEUP.reg |= EIC_WAKEUP_WAKEUPEN4; // Set External Interrupt Controller to use channel 4 (pin 6)
  EIC->WAKEUP.reg |= EIC_WAKEUP_WAKEUPEN5; // Set External Interrupt Controller to use channel 2 (pin A4)
  // EIC->WAKEUP.reg |= EIC_WAKEUP_WAKEUPEN2; // channel 2 (pin A0)

  PM->SLEEP.reg |= PM_SLEEP_IDLE_CPU; // Enable Idle0 mode - sleep CPU clock only
  //PM->SLEEP.reg |= PM_SLEEP_IDLE_AHB; // Idle1 - sleep CPU and AHB clocks
  //PM->SLEEP.reg |= PM_SLEEP_IDLE_APB; // Idle2 - sleep CPU, AHB, and APB clocks

  // It is either Idle mode or Standby mode, not both.
  SCB->SCR |= SCB_SCR_SLEEPDEEP_Msk; // Enable Standby or "deep sleep" mode
}

void sleepNow()
{
  Serial.println("MCU is going to sleep . . .");
  SysTick->CTRL &= ~SysTick_CTRL_TICKINT_Msk; //disable systick interrupt
  LowPower.standby();                         //enters sleep mode
  SysTick->CTRL |= SysTick_CTRL_TICKINT_Msk;  //Enable systick interrupt
}
