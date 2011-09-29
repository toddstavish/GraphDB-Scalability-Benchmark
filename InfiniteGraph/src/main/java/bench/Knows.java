package bench;

import com.infinitegraph.BaseEdge;

class Knows extends BaseEdge
{
    private String topic;

    public Knows(String topic)
    {
        setTopic(topic);
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

}