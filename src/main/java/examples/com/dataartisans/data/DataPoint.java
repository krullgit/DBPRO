package examples.com.dataartisans.data;

public class DataPoint<T> {

  private long timeStampMs;
  private T mf01;
  private T mf02;
  private T mf03;

  public DataPoint() {
    this.timeStampMs = 0;
    this.mf01 = null;
    this.mf02 = null;
    this.mf03 = null;
  }

  public DataPoint(long timeStampMs, T mf01, T mf02, T mf03) {
    this.timeStampMs = timeStampMs;
    this.mf01 = mf01;
    this.mf02 = mf02;
    this.mf03 = mf03;
  }

  public long getTimeStampMs() {
    return timeStampMs;
  }

  public void setTimeStampMs(long timeStampMs) {
    this.timeStampMs = timeStampMs;
  }

  public T getMf01() {
    return mf01;
  }
  public T getMf02() {
    return mf02;
  }
  public T getMf03() {
    return mf03;
  }

  public void setMf01(T mf01) {
    this.mf01 = mf01;
  }
  public void setMf02(T mf01) {
    this.mf02 = mf02;
  }
  public void setMf03(T mf01) {
    this.mf03 = mf03;
  }

  public <R> DataPoint<R> withNewValue(R mf01,R mf02,R mf03){
    return new DataPoint<>(this.getTimeStampMs(), mf01, mf02, mf03);
  }

  public <R> KeyedDataPoint<R> withNewKeyAndValue(String key, R mf01,R mf02,R mf03){
    return new KeyedDataPoint<>(key, this.getTimeStampMs(), mf01, mf02, mf03);
  }

  public KeyedDataPoint withKey(String key){
    return new KeyedDataPoint<>(key, this.getTimeStampMs(), this.getMf01(),this.getMf02(),this.getMf03());
  }

  @Override
  public String toString() {
    return "DataPoint(timestamp=" + timeStampMs + ", value=" + mf01 + ")";
  }
}
