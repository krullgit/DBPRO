package examples.com.dataartisans.data;

import java.io.Serializable;

public class KeyedDataPoint<T> extends DataPoint<T> implements Serializable {

  private String key;

  public KeyedDataPoint(){
    super();
    this.key = null;
  }

  public KeyedDataPoint(String key, long timeStampMs, T mf01, T mf02, T mf03) {
    super(timeStampMs, mf01, mf02,mf03);
    this.key = key;
  }

  @Override
  public String toString() {
    return getTimeStampMs() + "," + getKey() + "," + getMf01();
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public <R> KeyedDataPoint<R> withNewValue(R mf01, R mf02, R mf03){
    return new KeyedDataPoint<>(this.getKey(), this.getTimeStampMs(), mf01, mf02, mf03);
  }

}
