#include <Arduino.h>         // required before wiring_private.h
#include "wiring_private.h"  // pinPeripheral() function
#include <SPI.h>
#include <RH_RF95.h>
#include <SparkFun_u-blox_GNSS_Arduino_Library.h>
#include "Sodaq_DS3231.h"
#include <Wire.h>
SFE_UBLOX_GNSS myGNSS;

#define BUFLEN (5 * RH_RF95_MAX_MESSAGE_LEN)  //max size of data burst we can handle - (5 full RF buffers) - just arbitrarily large
#define RFWAITTIME 500                        //maximum milliseconds to wait for next LoRa packet - used to be 600 - may have been too long
#define RTCM_TIMEOUT 300000                   //5 minutes

char sitecode[6] = "UPMHN";  //logger name - sensor site code
int MIN_SAT = 20;            //binaba from 30
int AVE_COUNT = 1;           //12 counts

bool READ_FLAG = false;
bool UBX_INIT_FLAG = false;
uint8_t RX_LORA_FLAG = 0;
unsigned long start;

char dataToSend[200];
char Ctimestamp[13] = "";

// initialize LoRa global variables
uint8_t payload[RH_RF95_MAX_MESSAGE_LEN];
uint8_t len = sizeof(payload);
uint8_t buf[RH_RF95_MAX_MESSAGE_LEN];
uint8_t len2 = sizeof(buf);

#define DEBUG 1
#define RTCINTPIN 6
#define VBATPIN A7  //new copy
#define VBATEXT A5

// for feather m0
#define RFM95_CS 8
#define RFM95_RST 4
#define RFM95_INT 3
#define RF95_FREQ 433.0
RH_RF95 rf95(RFM95_CS, RFM95_INT);

#define UBXPWR 5
#define LED 13

#define BAUDRATE 115200
#define DUEBAUD 9600
#define DUESerial Serial1

void readTimeStamp() {
  DateTime now = rtc.now();  //get the current date-time
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

  ts.remove(0, 2);  //remove 1st 2 data in ts
  ts.toCharArray(Ctimestamp, 13);
}

float readTemp() {
  float temp;
  rtc.convertTemperature();
  temp = rtc.getTemperature();
  return temp;
}

float readBatteryVoltage(uint8_t ver) {
  float measuredvbat;
  if ((ver == 3) || (ver == 9) || (ver == 10) || (ver == 11)) {
    measuredvbat = analogRead(VBATPIN);  //Measure the battery voltage at pin A7
    measuredvbat *= 2;                   // we divided by 2, so multiply back
    measuredvbat *= 3.3;                 // Multiply by 3.3V, our reference voltage
    measuredvbat /= 1024;                // convert to voltage
    measuredvbat += 0.28;                // add 0.7V drop in schottky diode
  } else {
    /* Voltage Divider 1M and  100k */
    measuredvbat = analogRead(VBATEXT);
    measuredvbat *= 3.3;     // reference voltage
    measuredvbat /= 1024.0;  // adc max count
    measuredvbat *= 11.0;    // (100k+1M)/100k
  }
  return measuredvbat;
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

void init_ublox() {
  Wire.begin();
  digitalWrite(UBXPWR, HIGH);

  for (int x = 0; x < 10; x++) {  //10 retries to not exceed watchdog limit(16sec)
    if (myGNSS.begin(Wire) == false) {
      Serial.println(F("u-blox GNSS not detected at default I2C address. Please check wiring. Freezing."));
      delay(1000);
    } else {
      Serial.println("u-blox GNSS begin");
      break;
    }
  }
  myGNSS.setI2COutput(COM_TYPE_UBX);  //Set the I2C port to output UBX only (turn off NMEA noise)
  myGNSS.setNavigationFrequency(5);   //Set output to 20 times a second
  myGNSS.setHighPrecisionMode(true);
  myGNSS.powerSaveMode(true);
}

byte checkRTKFixType() {
  byte RTK = myGNSS.getCarrierSolutionType();
  Serial.print("RTK: ");
  Serial.print(RTK);
  if (RTK == 0) Serial.println(F(" (No solution)"));
  else if (RTK == 1) Serial.println(F(" (High precision floating fix)"));
  else if (RTK == 2) Serial.println(F(" (High precision fix)"));
  return RTK;
}

byte checkSatelliteCount() {
  byte SIV = myGNSS.getSIV();
  Serial.print("Sat #: ");
  Serial.println(SIV);
  return SIV;
}

float checkHorizontalAccuracy() {
  float HACC = myGNSS.getHorizontalAccuracy();
  Serial.print("Horizontal Accuracy: ");
  Serial.println(HACC);
  return HACC;
}

float checkVerticalAccuracy() {
  float VACC = myGNSS.getVerticalAccuracy();
  Serial.print("Vertical Accuracy: ");
  Serial.println(VACC);
  return VACC;
}

void getRTCM() {
  rf95.setModemConfig(RH_RF95::Bw500Cr45Sf128);  //lora config for send/receive rtcm
  uint8_t buf[BUFLEN];
  unsigned buflen;

  uint8_t rfbuflen;
  uint8_t *bufptr;
  unsigned long lastTime;

  bufptr = buf;
  if (rf95.available()) {
    digitalWrite(LED_BUILTIN, HIGH);
    rfbuflen = RH_RF95_MAX_MESSAGE_LEN;
    if (rf95.recv(bufptr, &rfbuflen)) {
      bufptr += rfbuflen;
      lastTime = millis();
      while (((millis() - lastTime) < RFWAITTIME) && ((bufptr - buf) < (BUFLEN - RH_RF95_MAX_MESSAGE_LEN))) {  //Time out or buffer can't hold anymore
        if (rf95.available()) {
          rfbuflen = RH_RF95_MAX_MESSAGE_LEN;
          if (rf95.recv(bufptr, &rfbuflen)) {
            Serial.println((unsigned char)*bufptr, HEX);
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
    buflen = (bufptr - buf);       //Total bytes received in all packets
    DUESerial.write(buf, buflen);  //Send data to the GPS -- Serial1
    digitalWrite(LED_BUILTIN, LOW);
  }
}

void getGNSSData(char *dataToSend, unsigned int bufsize) {
  for (int i = 0; i < 200; i++) {
    dataToSend[i] = (uint8_t)'\0';
  }

  getRTCM();

  if (checkRTKFixType() == 2 && checkSatelliteCount() >= MIN_SAT) {
    readUbloxData();
    getRTCM();
    // RX_LORA_FLAG == 1;
    // READ_FLAG = true;

    readTimeStamp();
    strncat(dataToSend, "*", 2);
    strncat(dataToSend, Ctimestamp, 13);
    Serial.println(dataToSend);
  }

  else if (((checkRTKFixType() != 2) || (checkSatelliteCount() < MIN_SAT))) {
    getRTCM();
  }

  // if (READ_FLAG = true) {
  //   READ_FLAG = false;
  //   RX_LORA_FLAG == 0;

  //   readTimeStamp();
  //   strncat(dataToSend, "*", 2);
  //   strncat(dataToSend, Ctimestamp, 13);
  //   Serial.println(dataToSend);
  // }
}

void readUbloxData() {
  for (int i = 0; i < 200; i++) {
    dataToSend[i] = (uint8_t)'\0';
  }

  byte rtk_fixtype = checkRTKFixType();
  int sat_num = checkSatelliteCount();

  // Defines storage for the lat and lon as double
  double d_lat = 0.0;  // latitude
  double d_lon = 0.0;  // longitude

  double accu_lat = 0.0;  // latitude accumulator
  double accu_lon = 0.0;  // longitude accumulator
  int accu_count = 0;

  // Now define float storage for the heights and accuracy
  float f_msl = 0.0;
  float f_accuracy_hor = 0.0;
  float f_accuracy_ver = 0.0;

  float accu_msl = 0.0;           //msl accumulator
  float accu_accuracy_hor = 0.0;  //hacc acuumulator
  float accu_accuracy_ver = 0.0;  //vacc accumulator

  char tempstr[100];
  char volt[10];
  char temp[10];

  snprintf(volt, sizeof volt, "%.2f", readBatteryVoltage(10));
  snprintf(temp, sizeof temp, "%.2f", readTemp());

  for (int j = 1; j <= AVE_COUNT; j++) {
    getRTCM();

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
    d_lat = ((double)latitude) / 10000000.0;        // Convert latitude from degrees * 10^-7 to degrees
    d_lat += ((double)latitudeHp) / 1000000000.0;   // Now add the high resolution component (degrees * 10^-9 )
    d_lon = ((double)longitude) / 10000000.0;       // Convert longitude from degrees * 10^-7 to degrees
    d_lon += ((double)longitudeHp) / 1000000000.0;  // Now add the high resolution component (degrees * 10^-9 )

    // Calculate the height above mean sea level in mm * 10^-1
    f_msl = (msl * 10) + mslHp;  // Now convert to m
    f_msl = f_msl / 10000.0;     // Convert from mm * 10^-1 to m

    // Convert the accuracy (mm * 10^-1) to a float
    f_accuracy_hor = hor_acc / 10000.0;  // Convert from mm * 10^-1 to m
    f_accuracy_ver = ver_acc / 10000.0;  // Convert from mm * 10^-1 to m

    // if ((checkHorizontalAccuracy() == 141 && checkVerticalAccuracy() <= 141)) {
    //   // Accumulation
    //   accu_lat += d_lat;
    //   accu_lon += d_lon;
    //   accu_msl += f_msl;
    //   accu_accuracy_hor += f_accuracy_hor;
    //   accu_accuracy_ver += f_accuracy_ver;
    //   accu_count++;
    //   Serial.print("accu_count: ");
    //   Serial.println(accu_count);

    // } else {
    //   i--; //loop until hacc&vacc conditions are satisfied or until timeout reached
    //   getRTCM();
    // }

    //NO CONDITIONS ON HACC & VACC
    accu_lat += d_lat;
    accu_lon += d_lon;
    accu_msl += f_msl;
    accu_accuracy_hor += f_accuracy_hor;
    accu_accuracy_ver += f_accuracy_ver;
  }

  // Averaging
  d_lat = accu_lat / accu_count;
  d_lon = accu_lon / accu_count;
  f_msl = accu_msl / accu_count;
  f_accuracy_hor = accu_accuracy_hor / accu_count;
  f_accuracy_ver = accu_accuracy_ver / accu_count;

  if ((d_lat > 0) || (d_lon > 0)) {
    sprintf(tempstr, ">>%s:%d,%.9f,%.9f,%.4f,%.4f,%.4f,%d", sitecode, rtk_fixtype, d_lat, d_lon, f_accuracy_hor, f_accuracy_ver, f_msl, sat_num);
    strncpy(dataToSend, tempstr, strlen(tempstr) + 1);
    strncat(dataToSend, ",", 2);
    strncat(dataToSend, temp, sizeof(temp));
    strncat(dataToSend, ",", 2);
    strncat(dataToSend, volt, sizeof(volt));
    // Serial.print("data to send: ");
    // Serial.println(dataToSend);
  } else {
    noGNSSDataAcquired();
  }

  d_lat, d_lon, f_msl, f_accuracy_hor, f_accuracy_ver = 0.0;
  accu_lat, accu_lon, accu_msl, accu_accuracy_hor, accu_accuracy_ver = 0.0;  //reset accumulators to zero
  accu_count = 0;
}

void noGNSSDataAcquired() {
  for (int i = 0; i < 200; i++) {
    dataToSend[i] = (uint8_t)'\0';
  }

  strncpy(dataToSend, ">>", 3);
  strncat(dataToSend, sitecode, sizeof(sitecode));
  strncat(dataToSend, ":No Ublox data.", 16);
  // Serial.print("data to send: ");
  // Serial.println(dataToSend);
}

// void ubloxInitFailed() {
//   for (int i = 0; i < 200; i++) {
//     dataToSend[i] = (uint8_t)'\0';
//   }

//   strncpy(dataToSend, ">>", 3);
//   strncat(dataToSend, sitecode, sizeof(sitecode));
//   strncat(dataToSend,":Ublox Init Failed.", 20);
//   Serial.print("data to send: ");
//   Serial.println(dataToSend);
// }

// void initialize_sitecode() {
//   if ((get_logger_mode() == 1) || (get_logger_mode() == 9)) { //Gateway with sensor and 1 lora tx (if gnss) ; Gateway rain gauge with gnss
//     char *logger_B_data = get_logger_B_from_flashMem();
//     strncpy(sitecode, logger_B_data, 5); // Copy up to 5 characters to avoid buffer overflow
//     sitecode[5] = '\0'; // Null-terminate the string
//   } else {
//     char *logger_A_data = get_logger_A_from_flashMem();
//     strncpy(sitecode, logger_A_data, 5); // Copy up to 5 characters to avoid buffer overflow
//     sitecode[5] = '\0'; // Null-terminate the string
//   }
// }


void setup() {
  DUESerial.begin(BAUDRATE);
  delay(100);

  Wire.begin();
  rtc.begin();

  pinMode(LED, OUTPUT);
  pinMode(RFM95_RST, OUTPUT);
  pinMode(UBXPWR, OUTPUT);
  delay(500);

  digitalWrite(LED_BUILTIN, LOW);
  digitalWrite(RFM95_RST, HIGH);
  digitalWrite(UBXPWR, LOW);

  Serial.println("Feather LoRa RX");
  digitalWrite(RFM95_RST, LOW);
  delay(100);
  digitalWrite(RFM95_RST, HIGH);
  delay(100);

  while (!rf95.init()) {
    Serial.println("LoRa radio init failed");
    while (1)
      ;
  }
  Serial.println("LoRa radio init OK!");

  if (!rf95.setFrequency(RF95_FREQ)) {
    Serial.println("setFrequency failed");
    while (1)
      ;
  }

  rf95.setTxPower(23, false);
  init_ublox();
}

void loop() {
  getGNSSData(dataToSend, sizeof(dataToSend));
  delay(100);
}