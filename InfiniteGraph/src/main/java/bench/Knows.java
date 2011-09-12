package bench;

import com.infinitegraph.BaseEdge;

class Knows extends BaseEdge
{
    private String topic;
    private int weight;

    public Knows(String topic, int weight)
    {
        setTopic(topic);
        setWeight(weight);
    }

    public void setTopic(String topic)
    {
        markModified();
        this.topic = topic;
    }

    public String getTopic()
    {
        fetch();
        return this.topic;
    }

    public void setWeight(int weight)
    {
        markModified();
        this.weight = weight;
    }

    public int getWeight()
    {
        fetch();
        return this.weight;
    }
}