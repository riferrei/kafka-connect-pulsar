/**

    Copyright © 2020 Ricardo Ferreira (riferrei@riferrei.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

package com.riferrei.kafka.connect.pulsar;

import java.nio.ByteBuffer;
import java.util.Map;

public class AvroComplexType  {

  private String stringField;
  private boolean booleanField;
  private ByteBuffer bytesField;
  private int intField;
  private long longField;
  private float floatField;
  private double doubleField;
  private Map<String, Double> mapField;
  private AvroInnerType innerField;

  public String getStringField() {
      return stringField;
  }

  public void setStringField(String stringField) {
      this.stringField = stringField;
  }

  public boolean isBooleanField() {
      return booleanField;
  }

  public void setBooleanField(boolean booleanField) {
      this.booleanField = booleanField;
  }

  public ByteBuffer getBytesField() {
      return bytesField;
  }

  public void setBytesField(ByteBuffer bytesField) {
      this.bytesField = bytesField;
  }

  public int getIntField() {
      return intField;
  }

  public void setIntField(int intField) {
      this.intField = intField;
  }

  public long getLongField() {
      return longField;
  }

  public void setLongField(long longField) {
      this.longField = longField;
  }

  public float getFloatField() {
      return floatField;
  }

  public void setFloatField(float floatField) {
      this.floatField = floatField;
  }

  public double getDoubleField() {
      return doubleField;
  }

  public void setDoubleField(double doubleField) {
      this.doubleField = doubleField;
  }

  public Map<String, Double> getMapField() {
      return mapField;
  }

  public void setMapField(Map<String, Double> mapField) {
      this.mapField = mapField;
  }

  public AvroInnerType getInnerField() {
      return innerField;
  }

  public void setInnerField(AvroInnerType innerField) {
      this.innerField = innerField;
  }

}
